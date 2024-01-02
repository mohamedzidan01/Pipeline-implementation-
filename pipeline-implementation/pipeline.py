import os 
import time 
import multiprocessing as mp
import psutil
import shutil
"""
    simple implementation of the concept of pipeline   
    a pipeline consisted of 4 stages 
    the input is 10 paths of empty .txt files 
    using four processes each represent a stage.
    
"""
"""
    the program simply takes a directory extracts any .txt file
    from it then apply four stages process over each of files
    and saves copy of the result after any stage
"""
#function to simulate the load of enormous processes 
#to manipulate the environment 
def sleep(min_ms, max_ms):
    """Sleeps for a random number of milliseconds between min_ms and max_ms."""
    sleep_time = random.randint(min_ms, max_ms) / 1000  
    time.sleep(sleep_time)
min_ms=0
max_ms=0
#to have a quick look on running processes
def copy(source_file, destination_path):
    try:
        destination_file = os.path.join(destination_path, os.path.basename(source_file))
        shutil.copy2(source_file, destination_file)  # Preserve metadata
    except FileNotFoundError:
        print(f"Error: Source file '{source_file}' not found.")
    except shutil.Error as e:
        print(f"Error copying file: {e}")
    except OSError as e:
        print(f"Error accessing files or directories: {e}")

#handle files before processing
def clear_txt_file(file_path):
    for file0 in file_path:
        try:
            with open(file0, "w") as file:
                file.truncate()  # Clear the file content efficiently
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
        except OSError as e:
            print(f"Error clearing file: {e}")
#same
def clear_directory(directory_path):
    try:
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Recursively remove subdirectories
    except FileNotFoundError:
        print(f"Error: Directory '{directory_path}' not found.")
    except OSError as e:
        print(f"Error clearing directory: {e}")


def stage1(path,queue):
    
    for x in path:
        copy(x,"data/stage1")  
        string_to_add = "stage1 completed."
        with open(x, "a") as file:
            file.write(string_to_add)
            file.write("\n")
        copy(x,"data/stage1")  
        queue.put(x)
def stage2(queue1,queue2):
    while queue1:
        path=queue1.get()
        string_to_add = "stage2 completed."
        with open(path, "a") as file:
            file.write(string_to_add)
            file.write("\n") 
        copy(path,"data/stage2") 
        queue2.put(path) 
    
def stage3(queue2,queue3):
    time.sleep(1)

    while queue2:
        path=queue2.get()
        string_to_add = "stage3 completed."
        with open(path, "a") as file:
            file.write(string_to_add)
            file.write("\n")  
        copy(path,"data/stage3")
        queue3.put(path)
        
def stage4(queue3):
    while queue3:
        path=queue3.get()
        string_to_add = "stage4 completed."
        with open(path, "a") as file:
            file.write(string_to_add)
            file.write("\n")  
        copy(path,"data/stage4")
        
#considered as simple communication between processes "data transportation"       
stage1_q=mp.Queue()
stage2_q=mp.Queue()    
stage3_q=mp.Queue()
file_list = []
    

for dirpath, dirnames, filenames in os.walk('data/stage0'):
    file_list += [os.path.join(dirpath, file) for file in filenames]
    
p1=mp.Process(target=stage1,args=(file_list,stage1_q))
p2=mp.Process(target=stage2,args=(stage1_q,stage2_q))
p3=mp.Process(target=stage3,args=(stage2_q,stage3_q))
p4=mp.Process(target=stage4,args=(stage3_q,))    
    
p1.start()
p2.start()

p3.start()
p4.start()
#check
#proof that it's parallel 
print(f'Are processes running: p1: {p1.is_alive()}, p2: {p2.is_alive()}, p3: {p3.is_alive()}, p4: {p4.is_alive()}')

p1.join()
p2.join()
p3.join()
p4.join()

print("All processes completed successfully")
#garbage collector
clear_txt_file(file_list)        
stages=["data/stage1","data/stage2","data/stage3","data/stage4","data/final"]
for stage in stages:
    clear_directory(stage)