import sys
import time
import json
import glob
from tqdm import tqdm
from google.oauth2.service_account import Credentials
from google.cloud import iam_admin_v1
from google.api_core import exceptions as api_exceptions

def find_target_service_accounts(client, project_id, prefix):
    """在项目中列出所有服务账号，并根据名称前缀进行筛选。"""
    print(f"🔍 正在项目 '{project_id}' 中查找前缀为 '{prefix}' 的服务账号...")
    
    target_accounts = []
    
    try:
        request = iam_admin_v1.ListServiceAccountsRequest(
            name=f"projects/{project_id}",
        )
        for account in client.list_service_accounts(request=request):
            account_name = account.email.split('@')[0]
            if account_name.startswith(prefix):
                target_accounts.append(account)
    except api_exceptions.PermissionDenied as e:
        print(f"\n❌ 权限不足: 无法列出服务账号。")
        print(f"   请确保您的控制器账号拥有 'iam.serviceAccounts.list' 权限。")
        print(f"   详细信息: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 查找账号时发生意外错误: {e}")
        sys.exit(1)
        
    return target_accounts

def delete_keys_for_account(client, sa_email, retries=3, backoff_factor=1.5):
    """为单个服务账号删除所有用户管理的密钥，并带有重试机制。"""
    last_exception = None
    
    for attempt in range(retries):
        try:
            keys_deleted_count = 0
            sa_full_name = f"projects/-/serviceAccounts/{sa_email}"
            
            list_request = iam_admin_v1.ListServiceAccountKeysRequest(
                name=sa_full_name,
                key_types=[iam_admin_v1.ListServiceAccountKeysRequest.KeyType.USER_MANAGED],
            )
            
            # *** 关键修复点：从响应对象中访问 .keys 属性 ***
            response = client.list_service_account_keys(request=list_request)
            key_names = [key.name for key in response.keys]

            if not key_names:
                return 0 # 成功，没有密钥需要删除

            for key_name in key_names:
                delete_request = iam_admin_v1.DeleteServiceAccountKeyRequest(name=key_name)
                client.delete_service_account_key(request=delete_request)
                keys_deleted_count += 1
                time.sleep(0.1) # 每次删除后短暂延时
            
            return keys_deleted_count # 成功

        except (api_exceptions.Aborted, api_exceptions.DeadlineExceeded, api_exceptions.ServiceUnavailable) as e:
            last_exception = e
            sleep_time = backoff_factor ** attempt
            tqdm.write(f"  - ⚠️  API暂时性错误 (尝试 {attempt + 1}/{retries})，正在等待 {sleep_time:.1f}秒 后重试...")
            time.sleep(sleep_time)
        except api_exceptions.GoogleAPICallError as e:
            tqdm.write(f"\n   - ❌ 处理 {sa_email} 时遇到严重错误: {e}")
            return -1 # 严重错误，不再重试

    tqdm.write(f"  - ❌ 为 {sa_email} 删除密钥失败，已达最大重试次数。最后错误: {last_exception}")
    return -1

def main():
    """主函数，驱动密钥删除流程。"""
    print("--- GCP 服务账号密钥删除工具 (仅删除) ---")
    
    # 1. 认证并从控制器文件中获取项目ID
    try:
        controller_files = glob.glob('credentials.json') + glob.glob('controller/*.json')
        if not controller_files:
            sys.exit("❌ 错误: 在当前目录或 'controller/' 目录中未找到凭证.json 文件。")
        controller_file = controller_files[0]
    except Exception as e:
        sys.exit(f"❌ 查找控制器文件时出错: {e}")

    with open(controller_file, 'r') as f:
        project_id = json.load(f).get('project_id')
        if not project_id:
            sys.exit("❌ 错误: 在控制器 JSON 文件中未找到 'project_id'。")

    credentials = Credentials.from_service_account_file(controller_file, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    iam_admin_client = iam_admin_v1.IAMClient(credentials=credentials)

    # 2. 从用户处获取名称前缀
    prefix = input(f"请输入项目 '{project_id}' 中服务账号的名称前缀 (例如, 'sa-prod'): ").strip()
    if not prefix:
        sys.exit("❌ 错误: 前缀不能为空。")

    # 3. 查找所有匹配前缀的服务账号
    target_accounts = find_target_service_accounts(iam_admin_client, project_id, prefix)

    if not target_accounts:
        print(f"\n✅ 未找到前缀为 '{prefix}' 的服务账号。无需任何操作。")
        sys.exit(0)

    print(f"\n找到 {len(target_accounts)} 个匹配前缀的服务账号:")
    for acc in target_accounts:
        print(f"  - {acc.email}")

    # 4. 在删除任何东西之前，要求用户明确确认
    print("\n" + "="*50)
    print("⚠️  警告: 此脚本将永久删除上面列出的服务账号的")
    print("   所有【用户管理的密钥】。此操作不可逆！")
    print("="*50)
    
    confirm = input("请输入 'yes' 以继续删除密钥: ").lower()
    if confirm!= 'yes':
        print("\n用户已中止操作。没有密钥被删除。")
        sys.exit(0)

    # 5. 使用进度条处理删除过程
    print("\n正在删除密钥...")
    total_keys_deleted = 0
    
    with tqdm(total=len(target_accounts), desc="处理SA中") as pbar:
        for account in target_accounts:
            sa_email = account.email
            pbar.set_postfix_str(sa_email, refresh=True)
            
            deleted_count = delete_keys_for_account(iam_admin_client, sa_email)
            
            if deleted_count > 0:
                total_keys_deleted += deleted_count
                tqdm.write(f"  - ✅ 已为 {sa_email} 删除 {deleted_count} 个密钥")
            elif deleted_count == 0:
                tqdm.write(f"  - ℹ️  未找到 {sa_email} 的用户管理密钥")
            
            pbar.update(1)
            time.sleep(0.2) 

    print("\n" + "="*50)
    print("🎉 删除过程完成！")
    print(f"   删除的密钥总数: {total_keys_deleted}")
    print("="*50)


if __name__ == '__main__':
    main()

