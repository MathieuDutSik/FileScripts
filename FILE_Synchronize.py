import sys
import time
import hashlib
import shutil
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

MapFile = dict() # Storing all the current version of the file.
RevMap = dict()   # Mapping the hash to the files.


TripleInfo = namedtuple('TripleInfo', 'filename hashv date')
FileDatabase = namedtuple('FileDatabase', 'MapFile RevMap')

LocalDatabase = FileDatabase(dict(), dict())
DistantDatabase = FileDatabase(dict(), dict())

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


def is_message_present(list_file):
    for e_file in list_file:
        if os.path.exists(e_file):
            return True
    return False

#
# Atomic operation.
#

def send_request(do_rename, prev_triple, e_triple):
    FileCopy1 = DropboxAddress + "FILEO_Meta_" + hostname
    FileCopy2 = DropboxAddress + "FILEO_data_" + hostname
    wait_for_non_exist_file([FileCopy1, FileCopy2])
    print("Synchronization files missing. We can proceed")
    #
    f = os.open(FileCopy1, 'w')
    if do_rename:
        f.write("1\n")
    else:
        f.write("2\n")
    f.write(prev_triple.filename) # previous file
    f.write(prev_triple.date) # previous hash
    f.write(prev_triple.hashv) # previous date
    #
    f.write(e_triple.filename) # previous file
    f.write(e_triple.date) # previous hash
    f.write(e_triple.hashv) # previous date
    f.close()
    #
    if not do_rename:
        shutil.copy(DirectorySync + e_file, FileCopy2)

def send_database(the_database):
    FileCopy1 = DropboxAddress + "FILEO_Meta_" + hostname
    FileCopy2 = DropboxAddress + "FILEO_data_" + hostname
    wait_for_non_exist_file([FileCopy1, FileCopy2])
    f = os.open(FileCopy1, 'w')
    f.write("3\n")
    f.write("irrelevant1\n");
    f.write("irrelevant2\n");
    f.write("irrelevant3\n");
    f.write("irrelevant4\n");
    f.write("irrelevant5\n");
    f.write("irrelevant6\n");
    f.close()
    #
    f = os.open(FileCopy2, 'w')
    for e_hash in the_database.RevMap.keys():
        e_ent = the_database.RevMap[e_hash]
        e_file = e_ent[0]
        e_date = e_ent[1]
        f.write(e_file, "\n")
        f.write(e_hash, "\n")
        f.write(e_date, "\n")
    f.close()



def recv_request(remote_hostname, my_database, oth_database):
    FileCopy1 = DropboxAddress + "FILEO_Meta_" + remote_hostname
    FileCopy2 = DropboxAddress + "FILEO_data_" + remote_hostname
    wait_for_exist_file([FileCopy1])
    f = os.open(FileCopy1, 'r')
    evalue = f.read()
    prev_file = f.read()
    prev_hash = f.read()
    prev_date = f.read()
    #
    e_file = f.read()
    e_hash = f.read()
    e_date = f.read()
    f.close()
    #
    if evalue == 1:
        # Receiving a move order
        if os.exists(e_file):
            print("The file e_file=", e_file, " exists, which is not allowed")
            os.exit(0)
        if not os.exists(prev_file):
            print("The file prev_file=", prev_file, " exists, which is not allowed")
            os.exit(0)
        shutil.move(DirectorySync + prev_file, DirectorySync + e_file)
        TheEnt = the_database,MapFile[prev_file]
        my_database.MapFile.pop(prev_file)
        my_database.MapFile[e_file] = TheEnt
        for e_ent in TheEnt:
            e_hash = e_ent[0]
            e_date = e_ent[1]
            my_database.RevMap[e_hash] = [e_file, e_date]
        os.remove(FileCopy1)
    if evalue == 2:
        # Receiving a new file.
        wait_for_exist_file([FileCopy2])
        if prev_file in MapFile.keys():
            last_ent = MapFile[prev_file][-1]
            if last_ent[0] == prev_hash and last_ent[1] == prev_date:
                os.system("mv " + FileCopy2 + " " + prevFile)
                my_database.MapFile[prev_file].append([e_hash, e_date])
                my_database.RevMap[e_hash] = [prev_file, e_date]
            else:
                print("Incoherent changes of the file database")
                sys.exit(0);
        else:
            shutil.move(FileCopy2,  DirectorySync + prev_file)
            my_database.MapFile[prev_file] = [ [e_hash, e_date] ]
            my_database.RevMap[e_hash] = [prev_file, e_date]
            #
        os.remove(FileCopy2)
    if evalue == 3:
        wait_for_exist_file([FileCopy2])
        f = os.open(FileCopy2, 'r')
        n_ent = f.read()
        for i_ent in range(n_ent):
            e_file = f.read()
            e_hash = f.read()
            e_date = f.read()
            oth_database.MapFile[e_file] = [ [e_hash, e_date] ]
            oth_database.RevMap[e_hash] = [e_file, e_date]
        f.close()


def is_triple_present(e_database, e_triple):
    e_file = e_triple.filename
    e_hash = e_triple.hashv
    e_date = e_triple.date
    if not(e_file in e_database.MapFile):
        return False
    last_ent = e_database.MapFile[e_file][-1]
    return last_ent == [e_hash, e_date]

def read_single_file(e_file, the_database):
    e_date = sys.date(e_file)
    e_hash = get_hash(e_file)
    the_database.RevMap[e_hash] = [e_file, e_date]
    if e_hash in the_database.RevMap and not(e_file in the_database.MapFile):
        # We have a renaming operation
        e_ent = the_database.RevMap[e_hash]
        e_ent_file = e_ent[0]
        e_ent_date = e_ent[1]
        my_list = the_database.MapFile[e_ent_file]
        the_database.MapFile[e_file] = my_list
        the_database.MapFile.erase(e_ent_file)
        the_database.RevMap[e_hash] = [e_file, e_date]
        prev_triple = TripleInfo(e_ent_file, e_hash, e_ent_date)
        e_triple = TripleInfo(e_file, e_hash, e_date)
        return ["rename", pre_triple, e_triple]
    if e_hash in the_database.RevMap and e_file in the_database.MapFile:
        return "nothing"
    e_ent = [e_hash, e_date]
    if e_file in the_database.MapFile:
        last_ent = the_database.MapFile[e_file][-1]
        e_prev_hash = last_ent[0]
        e_prev_date = last_ent[1]
        if e_prev_date != e_date or e_prev_hash != e_hash:
            prev_triple = TripleInfo(e_file, e_prev_hash, e_prev_date)
            e_triple    = TripleInfo(e_file, e_hash, e_date)
            the_database.MapFile[e_file].append(e_ent)
            the_database.RevMap[e_hash] = [e_file, e_date]
            the_database.RevMap.erase(e_prev_hash)
            return ["newversion", pre_triple, e_triple]
        return "nothing"
    else:
        the_database.MapFile[e_file] = [ e_ent ]
        the_databse.RevMap[e_hash] = [e_file, e_date]
        prev_triple = TripleInfo(e_file, e_hash, e_date)
        e_triple    = TripleInfo(e_file, e_hash, e_date)
        return ["newfile", prev_triple, e_triple]



list_operations = []

def read_all_files(e_dir, my_database, oth_database):
    list_infos = []
    for root, subdirs, files in os.walk(e_dir):
        list_infos.append( [root, subdirs, files])
    print("|list_infos|=", len(list_infos))
    for e_info in list_infos:
        e_file = e_info[2]
        e_operation = read_single_file(e_file, my_database)
        if e_operation != "nothing":
            e_triple = e_operation[2]
            if not is_triple_present(oth_database, e_triple):
                list_operations.append(e_operation)



read_all_files(DirectorySync, LocalDatabase)
send_database(LocalDatabase)

def get_remote_hostname():
    while(True):
        sys.sleep(10)
        for root, subdirs, files in os.walk(e_dir):
            LStr = files.split("FILEO_Meta")
            if len(LStr) == 2:
                e_hostname = LStr[1]
                if e_hostname != hostname:
                    return e_hostname

remote_hostname = get_remote_hostname()
print("Found remote_hostname=", remote_hostname)


#
# Some single_event operation.
#

def single_event(my_database, oth_database):
    # Doing a receive operation from the other side
    if is_message_present(list_file):
        print("We have a message pending, do it in priority")
        recv_request(remote_hostname, my_database, oth_database)
        return
    if len(list_operations) == 0:
        print("No pending operation to send, returning")
        return
    print("sending some request")
    e_operation = list_operations.pop()
    name_operation = e_operation[0]
    prev_triple = e_operation[1]
    e_triple = e_operation[2]
    if e_operation[0] == "rename":
        send_request(True, prev_triple, e_triple)
    else:
        send_request(False, prev_triple, e_triple)



#
# The never ending loop of operations
#

while True:
    sys.sleep(10)
    #
    single_event(my_database, oth_database)
