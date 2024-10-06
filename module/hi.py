# Input string
data = """
Akash,20,30,40,50,30
Kishore,34,68,40
"""
data_list = [line.split(',') for line in data.strip().split('\n')]
print(data_list)
