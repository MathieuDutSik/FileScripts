import sys
import time
import hashlib
from collections import namedtuple



hostname = os.environ["HOST"]
if hostname == "":
    hostname = os.environ["HOSTNAME"]


if hostname == "":
    print("We have found |hostname|=", len(hostname), " and hostname=", hostname)
    os.exit(0)

print("Finding the characteristic entry to be hostname=", hostname)


n_arg = len(sys.argv)
if n_arg != 1:
    print("The program is used as")
    print("python FILE_Synchronize.py [DirectorySync]")
    os.exit(0)

DirectorySync = sys.argv[1];
DropboxAddress = os.environ["HOME"] + "/Dropbox/Shared_synchronization_dir/"

MapFiles = dict()
RevMap = dict()

TripleInfo = namedtuple('TripleInfo', 'filename date hashv')
# triple: filename, hash, date


#
# Main basic routines of the system
#

def get_hash(filename):
    h = hashlib.sha1()
    with open(filename,'rb') as file:
       chunk = 0
       while chunk != b'':
           # read only 1024 bytes at a time
           chunk = file.read(1024)
           h.update(chunk)
   return h.hexdigest()


def wait_for_exist_file(list_file):
    while True:
        evalue = True
        for e_file in list_file:
            if not os.path.exists(e_file):
                evalue = False
        if evalue:
            return
        time.sleep(2)

def wait_for_non_exist_file(list_file):
    while True:
        evalue = True
        for e_file in list_file:
            if os.path.exists(e_file):
                evalue = False
        if evalue:
            return
        time.sleep(2)

#
# Atomic operation.
#

def send_request(do_rename, prev_triple, e_triple):
    FileCopy1 = DropboxAddress + "FILEO_Meta_" + hostname
    FileCopy2 = DropboxAddress + "FILEO_data_" + hostname
    while True:
        if not os.path.exists(FileCopy1) and not os.path.exists(FileCopy2):
            break
        time.sleep(2)
    print("Synchronization files missing. We can proceed")
    #
    os.open(FileCopy1, 'w')
    if do_rename:
        os.write("1\n")
    else:
        os.write("2\n")
    os.write(prev_triple.filename) # previous file
    os.write(prev_triple.date) # previous hash
    os.write(prev_triple.hashv) # previous date
    #
    os.write(e_triple.filename) # previous file
    os.write(e_triple.date) # previous hash
    os.write(e_triple.hashv) # previous date
    os.close()
    #
    if not do_rename:
        os.system("cp " + DirectorySync + e_file, " " + FileCopy2)

def recv_request(remote_hostname):
    FileCopy1 = DropboxAddress + "FILEO_Meta_" + remote_hostname
    FileCopy2 = DropboxAddress + "FILEO_data_" + remote_hostname
    os.open(FileCopy1, 'r')
    evalue = os.read()
    prev_file = os.read()
    prev_hash = os.read()
    prev_date = os.read()
    #
    e_file = os.read()
    e_hash = os.read()
    e_date = os.read()
    os.close()
    #
    if evalue == 2:
        if prev_file in MapFile.keys():
            last_ent = MapFile[prev_file][-1]
            if last_ent[0] == prev_hash and last_ent[1] == prev_date:
                os.system("mv " + FileCopy2 + " " + prevFile)
                MapFile[prev_file].append([e_hash, e_date])
            else:
                print("Incoherent changes of the file database")
                sys.exit(0);
        else:
            os.system("mv " + FileCopy2 + " " + prevFile)
            MapFile[prev_file] = [ [e_hash, e_date] ]
            #
        os.remove(FileCopy1)
        os.remove(FileCopy2)
    else:
        


def read_single_file(e_file):
    e_hash = get_hash(e_file)
    e_date = sys.date(e_file)
    RevMap[e_hash] = [e_file, e_date]
    if e_file in MapFiles.keys():
        MapFiles[e_file].append([e_hash, e_date] )
    else:
        MapFiles[e_file] = [  [e_hash, e_date]  ]
    e_triple = TripleInfo(e_file, e_date, e_hash)


def read_all_files(e_dir):
    list_infos = []
    for root, subdirs, files in os.walk(e_dir):
        list_infos.append( [root, subdirs, files])
    for e_info in list_infos:
        e_file = e_info[2]
        read_single_file(e_file)

while True:
    sys.sleep(10)
    #
    Look for file changes in the system.
    Two scenario:
    --- It is a file rename and so it is sent as a file rename to the other side.
    --- It is a new file or a new version. If so, send_file (only if the precedeing file has been processed)
    #
    Receive some message from other node.
    receive the entry.
