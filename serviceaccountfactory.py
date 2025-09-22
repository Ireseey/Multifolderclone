# 最终版：重构为分步批量处理逻辑，确保原子性和完整性 (已修复密钥数据解码问题)
# 步骤1: 批量检查和创建账号 (每创建4个后冷却20秒)
# 步骤2: 批量删除所有目标账号的旧密钥
# 步骤3 & 4: 批量为所有目标账号创建新密钥并写入文件 (每创建4个后冷却10秒)

from google.oauth2.service_account import Credentials
from google.cloud import iam_admin_v1
from google.api_core import exceptions as api_exceptions
import base64
import json
import glob
import sys
import os
import time
from tqdm import tqdm
import re

# --- 核心功能函数 (全部使用 iam_admin_v1 API) ---

def delete_all_user_keys_with_retry(iam_admin_client, project_id, sa_email, retries=3, backoff_factor=2):
    """
    【已验证成功的逻辑】为单个服务账号删除所有用户管理的密钥，并带有重试机制。
    """
    last_exception = None
    sa_full_name = f"projects/{project_id}/serviceAccounts/{sa_email}"
    
    for attempt in range(retries):
        try:
            list_request = iam_admin_v1.ListServiceAccountKeysRequest(
                name=sa_full_name,
                key_types=[iam_admin_v1.ListServiceAccountKeysRequest.KeyType.USER_MANAGED],
            )
            
            response = iam_admin_client.list_service_account_keys(request=list_request)
            key_names = [key.name for key in response.keys]

            if not key_names:
                return True # 成功，没有密钥需要删除

            for key_name in key_names:
                iam_admin_client.delete_service_account_key(name=key_name)
                time.sleep(0.05) # 轻微延迟以避免触发速率限制
            
            return True

        except (api_exceptions.Aborted, api_exceptions.DeadlineExceeded, api_exceptions.ServiceUnavailable, api_exceptions.RetryError) as e:
            last_exception = e
            sleep_time = backoff_factor ** attempt
            print(f"  - ⚠️  为 {sa_email} 删除密钥时API暂时性错误 (尝试 {attempt + 1}/{retries})，等待 {sleep_time:.1f}秒 后重试...")
            time.sleep(sleep_time)
        except Exception as e:
            print(f"  - ❌ 为 {sa_email} 清理旧密钥时遇到未知问题: {e}")
            return False

    print(f"  - ❌ 为 {sa_email} 删除旧密钥失败，已达最大重试次数。最后错误: {last_exception}")
    return False

def create_service_account(iam_admin_client, project_id, prefix, number):
    """仅创建一个服务账号，不创建密钥。"""
    sa_name = f"{prefix}-{number:03d}"
    try:
        sa_request = iam_admin_v1.CreateServiceAccountRequest(
            name=f"projects/{project_id}",
            account_id=sa_name,
            service_account=iam_admin_v1.ServiceAccount(display_name=sa_name),
        )
        iam_admin_client.create_service_account(request=sa_request)
        time.sleep(0.5) # 等待SA创建后在API中可见
        return True
    except api_exceptions.AlreadyExists:
        return True # 已存在视为成功
    except Exception as e:
        print(f"  - ‼️ 创建新账号 {sa_name} 时出错: {e}")
        return False

def create_and_save_key(iam_admin_client, project_id, sa_email, number):
    """为指定SA创建一个新密钥并保存到文件。"""
    sa_filename = os.path.join("accounts", f"{number}.json")
    sa_full_name = f"projects/{project_id}/serviceAccounts/{sa_email}"
    try:
        key_request = iam_admin_v1.CreateServiceAccountKeyRequest(
            name=sa_full_name,
            private_key_type=iam_admin_v1.types.ServiceAccountPrivateKeyType.TYPE_GOOGLE_CREDENTIALS_FILE,
        )
        key = iam_admin_client.create_service_account_key(request=key_request)
        
        # 【已最终修正】客户端库已自动完成Base64解码, private_key_data是原始JSON字节流。
        # 我们只需将其从bytes解码为utf-8字符串即可，无需再进行base64.b64decode。
        key_content_as_string = key.private_key_data.decode('utf-8')
        
        with open(sa_filename, "w", encoding="utf-8") as f:
            f.write(key_content_as_string)
            
        return True
    except api_exceptions.InvalidArgument as e:
        if 'key limit reached' in str(e).lower():
            print(f"  - ⚠️  账号 {sa_email} 密钥已达上限，这不应该发生。请检查权限或GCP延迟。")
        else:
            print(f"  - ‼️ 为 {sa_email} 创建密钥时遇到API参数错误: {e}")
        return False
    except Exception as e:
        # 此处捕获之前的 'utf-8' codec, 'Incorrect padding' 等所有未知错误
        print(f"  - ‼️ 为 {sa_email} 创建密钥时遇到未知错误: {e}")
        return False

# --- 主程序逻辑 ---
def main():
    try:
        controller_file = glob.glob('credentials.json') + glob.glob('controller/*.json')
        if not controller_file: raise IndexError
        controller_file = controller_file[0]
    except IndexError:
        sys.exit("❌ 错误：在当前目录或 'controller' 文件夹中找不到凭证.json文件。")

    with open(controller_file, 'r') as f:
        project_id = json.load(f)['project_id']
    print(f"✅ 检测到主控项目ID: {project_id}")
    
    prefix = input('▶️ 请输入服务账号的【名称前缀】(e.g., sa): ').lower().strip()
    total_target_count = int(input(f'▶️ 请输入 "{prefix}" 前缀账号的【目标总数量】: '))

    print("\n[模式] 将严格按照以下步骤执行：")
    print("1. 确保所有目标SA存在 -> 2. 清空所有SA的旧密钥 -> 3. 为所有SA创建新密钥")
    if input("❓ 这是一个严谨的同步操作，确定要继续吗? (请输入 'y' 确认): ").lower() != 'y':
        sys.exit("操作已取消。")

    credentials = Credentials.from_service_account_file(controller_file, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    iam_admin_client = iam_admin_v1.IAMClient(credentials=credentials)
    
    os.makedirs('accounts', exist_ok=True)
    
    target_sa_names = {f"{prefix}-{i:03d}" for i in range(1, total_target_count + 1)}
    target_sa_emails = {f"{name}@{project_id}.iam.gserviceaccount.com" for name in target_sa_names}

    # =================================================================================
    # 第一步：检查并创建缺失的服务账号
    # =================================================================================
    print("\n--- 步骤 1/3: 检查并创建服务账号 ---")
    while True:
        try:
            print("  - 正在从GCP获取现有的SA列表...")
            all_sas_in_project = iam_admin_client.list_service_accounts(name=f"projects/{project_id}")
            existing_sa_emails = {sa.email for sa in all_sas_in_project}
            
            missing_sa_emails = target_sa_emails - existing_sa_emails
            
            if not missing_sa_emails:
                print("  - ✅ 所有目标服务账号均已存在。")
                break

            print(f"  - ℹ️  检测到 {len(missing_sa_emails)} 个缺失的账号，开始创建...")
            
            sa_to_create = []
            for email in missing_sa_emails:
                match = re.match(rf"({prefix}-(\d{{3}}))@", email)
                if match:
                    number = int(match.group(2))
                    sa_to_create.append(number)

            total_to_create = len(sa_to_create)
            with tqdm(total=total_to_create, desc="创建账号") as pbar:
                for i, number in enumerate(sorted(sa_to_create)):
                    create_service_account(iam_admin_client, project_id, prefix, number)
                    pbar.update(1)
                    if (i + 1) % 4 == 0 and (i + 1) < total_to_create:
                        pbar.set_postfix_str("每4个冷却20秒...")
                        time.sleep(20)
                        pbar.set_postfix_str("")

            print("  - 创建完成，正在重新验证...")
            time.sleep(5)

        except Exception as e:
            print(f"  - ‼️ 在步骤1中发生严重错误: {e}，将在10秒后重试...")
            time.sleep(10)

    # =================================================================================
    # 第二步：删除所有目标账号的密钥
    # =================================================================================
    print("\n--- 步骤 2/3: 清理所有目标账号的旧密钥 ---")
    key_deletion_queue = list(target_sa_emails)
    while key_deletion_queue:
        failed_tasks = []
        with tqdm(total=len(key_deletion_queue), desc="删除密钥") as pbar:
            for sa_email in key_deletion_queue:
                if not delete_all_user_keys_with_retry(iam_admin_client, project_id, sa_email):
                    failed_tasks.append(sa_email)
                pbar.update(1)
        
        if not failed_tasks:
            print("  - ✅ 成功清理所有目标账号的密钥。")
            break
        else:
            print(f"  - ⚠️  有 {len(failed_tasks)} 个账号的密钥清理失败，将在10秒后重试...")
            key_deletion_queue = failed_tasks
            time.sleep(10)

    # =================================================================================
    # 第三步 & 第四步：为每个账号创建新密钥并写入文件
    # =================================================================================
    print("\n--- 步骤 3/3: 创建新密钥并写入本地文件 ---")
    key_creation_queue = list(sorted(target_sa_names))
    while key_creation_queue:
        failed_tasks = []
        total_in_queue = len(key_creation_queue)
        with tqdm(total=total_in_queue, desc="创建密钥") as pbar:
            for i, sa_name in enumerate(key_creation_queue):
                number = int(sa_name.split('-')[-1])
                sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
                if not create_and_save_key(iam_admin_client, project_id, sa_email, number):
                    failed_tasks.append(sa_name)
                pbar.update(1)
                if (i + 1) % 4 == 0 and (i + 1) < total_in_queue:
                    pbar.set_postfix_str("每4个冷却10秒...")
                    time.sleep(10)
                    pbar.set_postfix_str("")

        if not failed_tasks:
            print("  - ✅ 成功为所有目标账号创建了新密钥。")
            break
        else:
            print(f"  - ⚠️  有 {len(failed_tasks)} 个账号的新密钥创建失败，将在10秒后重试...")
            key_creation_queue = failed_tasks
            time.sleep(10)


    print("\n\n🎉 全部任务完成！")
    print(f">>> 已为项目 {project_id} 中的 {total_target_count} 个 '{prefix}-*' 账号确保了本地有一个全新的密钥文件。")
    print(">>> 所有密钥文件均保存在 'accounts' 文件夹中。")


if __name__ == '__main__':
    main()
