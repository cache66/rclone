#!/usr/bin/env python3
"""
测试 rclone 任务管理功能
"""

import json
import requests
import time

# RC 服务器地址
RC_URL = "http://localhost:5574"

# 测试创建异步任务
def test_create_async_job():
    print("=== 测试创建异步任务 ===")
    
    # 创建测试目录
    import os
    os.makedirs("test_source", exist_ok=True)
    os.makedirs("test_dest", exist_ok=True)
    
    # 创建测试文件
    with open("test_source/test1.txt", "w") as f:
        f.write("test content 1")
    with open("test_source/test2.txt", "w") as f:
        f.write("test content 2")
    
    # 准备请求数据
    data = {
        "srcFs": os.path.abspath("test_source"),
        "dstFs": os.path.abspath("test_dest"),
        "createEmptySrcDirs": True,
        "_async": True
    }
    
    # 发送请求
    response = requests.post(f"{RC_URL}/sync/copy", json=data)
    print(f"Response status: {response.status_code}")
    print(f"Response content: {response.text}")
    
    if response.status_code == 200:
        result = response.json()
        job_id = result.get("jobid")
        if job_id:
            print(f"Created job with ID: {job_id}")
            return job_id
    
    return None

# 测试获取任务状态
def test_get_job_status(job_id):
    print(f"\n=== 测试获取任务状态 (ID: {job_id}) ===")
    
    data = {"jobid": job_id}
    response = requests.post(f"{RC_URL}/job/status", json=data)
    
    print(f"Response status: {response.status_code}")
    print(f"Response content: {response.text}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"Job finished: {result.get('finished')}")
        print(f"Job success: {result.get('success')}")
        print(f"Job duration: {result.get('duration')} seconds")
        return result
    
    return None

# 测试列出所有任务
def test_list_jobs():
    print("\n=== 测试列出所有任务 ===")
    
    response = requests.post(f"{RC_URL}/job/list")
    
    print(f"Response status: {response.status_code}")
    print(f"Response content: {response.text}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"Total jobs: {len(result.get('jobids', []))}")
        print(f"Running jobs: {result.get('runningIds', [])}")
        print(f"Finished jobs: {result.get('finishedIds', [])}")
        return result
    
    return None

# 测试重复任务检测
def test_duplicate_job_detection():
    print("\n=== 测试重复任务检测 ===")
    
    import os
    
    # 准备相同的请求数据
    data = {
        "srcFs": os.path.abspath("test_source"),
        "dstFs": os.path.abspath("test_dest"),
        "createEmptySrcDirs": True,
        "_async": True
    }
    
    # 发送请求
    response = requests.post(f"{RC_URL}/sync/copy", json=data)
    print(f"Response status: {response.status_code}")
    print(f"Response content: {response.text}")
    
    if response.status_code == 200:
        result = response.json()
        job_id = result.get("jobid")
        if job_id:
            print(f"Created job with ID: {job_id}")
            return job_id
    
    return None

# 主测试函数
def main():
    print("开始测试 rclone 任务管理功能...\n")
    
    # 测试 1: 创建异步任务
    job_id1 = test_create_async_job()
    if not job_id1:
        print("测试失败: 无法创建异步任务")
        return
    
    # 等待任务完成
    time.sleep(3)
    
    # 测试 2: 获取任务状态
    test_get_job_status(job_id1)
    
    # 测试 3: 列出所有任务
    test_list_jobs()
    
    # 等待任务保存到持久化文件
    time.sleep(2)
    
    # 测试 4: 重复任务检测
    job_id2 = test_duplicate_job_detection()
    if job_id2:
        print(f"重复任务检测结果: job_id1={job_id1}, job_id2={job_id2}")
        if job_id1 == job_id2:
            print("✓ 成功: 重复任务使用了相同的 ID")
        else:
            print("✗ 失败: 重复任务使用了不同的 ID")
    
    # 测试 5: 检查任务文件是否生成
    import os
    if os.path.exists("rclone_jobs.json"):
        print("\n=== 检查任务文件 ===")
        print("✓ 成功: 任务文件 rclone_jobs.json 已生成")
        with open("rclone_jobs.json", "r") as f:
            jobs_data = json.load(f)
            print(f"任务文件包含 {len(jobs_data)} 个任务")
            for job in jobs_data:
                print(f"  - Job ID: {job['id']}, Finished: {job['finished']}, Success: {job['success']}")
    else:
        print("\n✗ 失败: 任务文件 rclone_jobs.json 未生成")
    
    print("\n测试完成!")

if __name__ == "__main__":
    main()
