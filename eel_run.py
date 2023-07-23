import eel

folder_path = "D:\\test_data"
fscon = eel.eel(folder_path)
# print(fscon.file_hierarchy)
# print(fscon.configs)
print(fscon.container_hierarchy)
# print(fscon.taskflow)

# if __name__ == "__main__":
#     fscon.process_tasks()