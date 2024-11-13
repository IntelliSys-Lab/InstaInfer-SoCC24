import sys

if __name__ == "__main__":
    while True:
        line = sys.stdin.readline().strip()  # 读取一行输入
        print(f"Received: {line}")  # 打印接收到的内容
        sys.stdout.flush()