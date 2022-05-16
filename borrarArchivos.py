import sys
from timeit import default_timer 
import os
import time

def BorrarArchivos():
      comienzo=default_timer()
      #current_time = time.time()
      print() 
      count=0
      for f in os.listdir("images/señal/voltage/"):
                 #print(f)
                 count=count+1
                 
      print(count)
      if(count>10):
            for i in os.listdir("images/señal/voltage"):
                      print(i)
                      os.remove(f'images/señal/voltage/{i}')

                
                         
folder = "/home/pi/Desktop/IOTSER/images1/"
compress_older_days = 5

now = time.time()
 
 
def sanitize_files():
    # Loop through all the folder
    for file in os.listdir(folder): 
        f = os.path.join(folder,file)
        if not f.endswith('.gz'):
            if os.stat(f).st_mtime < now - (60*60*24*compress_older_days) and os.path.isfile(f):
                print ("...Compressing file "+f)
                out_filename = f + ".gz"
                 
                f_in = open(f, 'rb')
                s = f_in.read()
                f_in.close()
 
                f_out = gzip.GzipFile(out_filename, 'wb')
                f_out.write(s)
                f_out.close()
                # Remove original uncompressed file
                os.remove(f)     
        # We ensure that the file we are going to delete has been compressed before
        

def sanitize_files2():
    delete_older_days = 10
    # Loop through all the folder
    for file in os.listdir(folder): 
        f = os.path.join(folder,file)
        print ("...estrando"+f)
        if os.stat(f).st_mtime < now - (60*60*24*delete_older_days) and os.path.isfile(f):
                print ("...eliminando file "+f)
                # Remove original uncompressed file
                #os.remove(f) 
                os.system(f"sudo rm {f}")    

def sanitize_files3():
     
     path = f"/home/pi/Desktop/MedidorMonofasico/Ser-Raspberry/images1/"
     now = time.time()
     for i in path:
         for f in os.listdir(path):
            f = os.path.join(path, f)
            print(f"paso for {f}")
            if os.stat(os.path.join(path,f)).st_mtime < now - 4 * 86400:
                 #print("paso if")
                 if os.path.isfile(f):
                     print("paso if 2")
                     os.remove(f)

     path = f"/home/pi/Desktop/MedidorMonofasico/Ser-Raspberry/images2/"
     now = time.time()
     for i in path:
         for f in os.listdir(path):
            f = os.path.join(path, f)
            print(f"paso for {f}")
            if os.stat(os.path.join(path,f)).st_mtime < now - 4 * 86400:
                 #print("paso if")
                 if os.path.isfile(f):
                     print("paso if 2")
                     os.remove(f)
     path = f"/home/pi/Desktop/MedidorMonofasico/Ser-Raspberry/images3/"
     now = time.time()
     for i in path:
         for f in os.listdir(path):
            f = os.path.join(path, f)
            print(f"paso for {f}")
            if os.stat(os.path.join(path,f)).st_mtime < now - 4 * 86400:
                 #print("paso if")
                 if os.path.isfile(f):
                     print("paso if 2")
                     os.remove(f)
    

#sanitize_files()
#sanitize_files2()
#sanitize_files3()

BorrarArchivos()

"""        
def EnviarImagenes():
    #oldepoch = time.time()
    current_time = time.time()
    #st = datetime.datetime.fromtimestamp(oldepoch).strftime('%Y-%m-%d')
    #print(f' st Enviar Imagenes: {st}')
    for f in os.listdir('/home/pi/Desktop/IOTSER/images1/'):
            creation_time = os.path.getctime(f)
            if (((current_time - creation_time) // (24 * 3600)) >= 7):
                os.unlink(f)

EnviarImagenes()
"""