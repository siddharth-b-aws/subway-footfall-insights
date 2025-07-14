import subprocess

def find_java():
    try:
        result = subprocess.run(["java", "-XshowSettings:properties", "-version"],
                                stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        for line in result.stderr.splitlines():
            if "java.home" in line:
                print(line.strip())
                return
        print("java.home not found in output")
    except FileNotFoundError:
        print("Java not found.")

find_java()
