# 最终版：自动发现并为所有匹配前缀的SA创建新密钥（不删除旧密钥）
# 流程: 1. 发现所有目标SA -> 2. 为每个SA创建并保存一个新密钥

from google.oauth2.service_account import Credentials
from google.cloud import iam_admin_v1
from google.api_core import exceptions as api_exceptions
import json
import glob
import sys
import os
import time
from tqdm import tqdm

# --- 核心功能函数 ---

def create_and_save_key(iam_admin_client, project_id, sa_email, sa_name):
    """为指定SA创建一个新密钥并以SA名称保存到文件。"""
    sa_filename = os.path.join("accounts", f"{sa_name}.json")
    sa_full_name = f"projects/{project_id}/serviceAccounts/{sa_email}"
    try:
        key_request = iam_admin_v1.CreateServiceAccountKeyRequest(
            name=sa_full_name,
            private_key_type=iam_admin_v1.types.ServiceAccountPrivateKeyType.TYPE_GOOGLE_CREDENTIALS_FILE,
        )
        key = iam_admin_client.create_service_account_key(request=key_request)
        
        # 客户端库已自动完成Base64解码, private_key_data是原始JSON字节流，直接解码即可
        key_content_as_string = key.private_key_data.decode('utf-8')
        
        with open(sa_filename, "w", encoding="utf-8") as f:
            f.write(key_content_as_string)
            
        return True
    except api_exceptions.InvalidArgument as e:
        # 当密钥达到10个的上限时，GCP会报此错误
        if 'key limit reached' in str(e).lower():
            print(f"  - ⚠️  账号 {sa_email} 密钥已达上限 (10个)，无法创建新密钥。")
        else:
            print(f"  - ‼️ 为 {sa_email} 创建密钥时遇到API参数错误: {e}")
        # 即使无法创建，也视为“已处理”，以便主循环继续，而不是重试
        return True
    except Exception as e:
        print(f"  - ‼️ 为 {sa_email} 创建或保存密钥时遇到未知错误: {e}")
        return False

# --- 主程序逻辑 ---
def main():
    try:
        # 查找主控文件
        controller_file = glob.glob('credentials.json') + glob.glob('controller/*.json')
        if not controller_file:
            raise IndexError
        controller_file = controller_file[0]
    except IndexError:
        sys.exit("❌ 错误：在当前目录或 'controller' 文件夹中找不到凭证.json文件。")

    with open(controller_file, 'r') as f:
        project_id = json.load(f)['project_id']
    print(f"✅ 检测到主控项目ID: {project_id}")
    
    prefix = input('▶️ 请输入服务账号的【名称前缀】(例如: my-sa, dev-runner): ').lower().strip()
    if not prefix:
        sys.exit("❌ 错误：前缀不能为空。")

    credentials = Credentials.from_service_account_file(controller_file, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    iam_admin_client = iam_admin_v1.IAMClient(credentials=credentials)
    
    os.makedirs('accounts', exist_ok=True)
    
    # --- 自动发现所有匹配前缀的服务账号 ---
    print(f"\n🔍 正在查找所有前缀为 '{prefix}' 的服务账号...")
    target_sas = []
    try:
        all_sas_in_project = iam_admin_client.list_service_accounts(name=f"projects/{project_id}")
        for sa in all_sas_in_project:
            # sa.email 的格式是 "account-id@project-id.iam.gserviceaccount.com"
            account_id = sa.email.split('@')[0]
            if account_id.startswith(prefix):
                # 存储账号ID和邮箱的字典
                target_sas.append({'name': account_id, 'email': sa.email})
    except Exception as e:
        sys.exit(f"❌ 查找服务账号列表时出错: {e}")

    if not target_sas:
        sys.exit(f"🤷 未在项目 {project_id} 中找到任何以 '{prefix}' 开头的服务账号。")
        
    total_found_count = len(target_sas)
    print(f"✅ 成功找到 {total_found_count} 个匹配的服务账号。")
    print("\n[模式] 将为所有找到的账号创建新密钥（不删除旧密钥）。")

    # =================================================================================
    # 核心步骤：为每个账号创建新密钥并写入文件
    # =================================================================================
    print("\n--- 开始为所有目标账号创建新密钥 ---")
    # 按名称排序以获得一致的执行顺序
    key_creation_queue = sorted(target_sas, key=lambda x: x['name'])
    while key_creation_queue:
        failed_tasks = []
        total_in_queue = len(key_creation_queue)
        with tqdm(total=total_in_queue, desc="创建密钥") as pbar:
            for i, sa_info in enumerate(key_creation_queue):
                if not create_and_save_key(iam_admin_client, project_id, sa_info['email'], sa_info['name']):
                    failed_tasks.append(sa_info) # 只有发生未知错误时才需要重试
                pbar.update(1)
                # 应用速率限制，防止API调用过于频繁
                if (i + 1) % 4 == 0 and (i + 1) < total_in_queue:
                    pbar.set_postfix_str("每4个冷却10秒...")
                    time.sleep(10)
                    pbar.set_postfix_str("")

        if not failed_tasks:
            print("  - ✅ 已为所有目标账号处理完毕。")
            break
        else:
            print(f"  - ⚠️  有 {len(failed_tasks)} 个账号因未知错误创建失败，将在10秒后重试...")
            key_creation_queue = failed_tasks
            time.sleep(10)


    print("\n\n🎉 全部任务完成！")
    print(f">>> 已为项目 {project_id} 中所有 {total_found_count} 个 '{prefix}*' 前缀的账号尝试创建新密钥。")
    print(">>> 所有成功创建的密钥文件均保存在 'accounts' 文件夹中。")


if __name__ == '__main__':
    main()
