import os
import json
import uuid
import threading 
import time
import sys
 

class Dataclass:

    def __init__(self, filename = 'default') :
        if filename == 'default' :
            self.filename = 'Datafile/data_' + uuid.uuid4().hex + '.json'
            self.time_file = 'Datafile/time_' + uuid.uuid4().hex + '.json'
            self.dictionary = dict()
            self.ttl = dict()
            self.lock = threading.Lock()

            with open(self.filename, 'w') as f :
                json.dump(self.dictionary, f)
                f.close()

            with open(self.time_file, 'w') as t:
                json.dump(self.ttl, t)
                t.close()

        elif os.path.exists(filename) :
            self.lock = threading.Lock()
            pass
        else:
            self.filename = 'Datafile/'+'data_'+ filename + '.json'
            self.time_file = 'Datafile/'+'time_'+ filename + '.json'
            self.dictionary = dict()
            self.ttl = dict()
            self.lock = threading.Lock()

            with open(self.filename, 'w') as f :
                json.dump(self.dictionary, f, indent=4)
                f.close()

            with open(self.time_file, 'w') as t :
                json.dump(self.ttl, t, indent=4)
                t.close()
        
    def create(self, key, value, sec=25) :
        self.lock.acquire()

        with open(self.filename) as f :
            data = json.load(f)
            f.close()

        with open(self.time_file) as t :
            time_data = json.load(t)
            t.close()

        if len(data)<(1024*1020*1024) and sys.getsizeof(value)<(16*1024):
            if key not in data:
                key = str(key)
                data[key] = value
                time_data[key] = (time.time(), sec)

                with open(self.filename, 'w') as f :
                    json.dump(data, f, indent=4)
                    f.close()

                with open(self.time_file, 'w') as t :
                    json.dump(time_data, t, indent=4)
                    t.close()

                self.lock.release()
                return key
            else:
                raise Exception('key already present in dictionary')
        else:
            raise Exception('file size exceeded or value is gretaer than 16 kb')

    
    def read(self, key) :
        self.lock.acquire()
        with open(self.filename) as f :
            data = json.load(f)
            f.close()

        with open(self.time_file) as t :
            time_data = json.load(t)
            t.close()

        key = str(key)

        if data.get(key) == None :
            self.lock.release()
            raise Exception('no such key present in dictionary')
        
        if (time.time() - time_data[key][0]) > time_data[key][1] :
            del data[key]
            del time_data[key]

            with open(self.filename, 'w') as f :
                json.dump(data, f, indent=4)
                f.close()

            self.lock.release()
            raise Exception('no such key present in dictionary')
        
        self.lock.release()
        return data[key]


    def delete(self, key) :
        self.lock.acquire()
        with open(self.filename) as f :
            data = json.load(f)
            f.close()

        with open(self.time_file) as t :
            time_data = json.load(t)
            t.close()

        key = str(key)

        if key not in data:
            self.lock.release()
            raise Exception('no such key present in dictionary')

        if (time.time() - time_data[key][0]) > time_data[key][1] :
            del data[key]
            del time_data[key]
            with open(self.filename, 'w') as f :
                json.dump(data, f, indent=4)
                f.close()

            self.lock.release()
            raise Exception('no such key present in dictionary')

        del data[key]
        del time_data[key]
        
        with open(self.filename, 'w') as f :
            json.dump(data, f, indent=4)
            f.close()

        with open(self.time_file, 'w') as t :
            json.dump(time_data, t, indent=4)
            t.close()

        self.lock.release()
        return key
