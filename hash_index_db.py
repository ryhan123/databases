import os
import typing
import json
from collections import OrderedDict

"""
    Hash Index type Database

        - Log file is .seg{SEG_ID}
        - Log broken up into different segments
        - Key-val pairs are entered into a segment until the segment fills, meaning all pairings in a recent
            segment are more recent than those in an earlier segment
        - 
        - Hash table storing keys and offsets are in table

            -- R1: simple key|dict/json storage

            -- R2: given max log segment size, when max size gets exceeded, start writing to new 


        - db directory structure

        - db_dir_name :
            - .init
                - contains serialized ordered list of seg ids
"""

class HashIndexDB:


    def __init__(self, dir_name: str = 'test_db', seg_file_name: str = '.seg', max_seg_size: int = 10):

        self._seg_file_name = seg_file_name
        self._max_seg_size = max_seg_size
        self._curr_seg_size = 0
        self._curr_seg_id = 0
        
        self._dir_name = dir_name
        # Maps seg id to hash index for seg
        self._closed_seg_table = OrderedDict()
        self._index_table = {}
        
        if not os.path.isdir(dir_name):

            os.mkdir(dir_name)
        
        if not os.path.isdir(os.path.join(dir_name, 'segs')):
            os.mkdir(os.path.join(dir_name, 'segs'))
        
        """
        if os.path.isdir(dir_name):

            self.reinitialize_db()

        else:

            self.initialize_db(dir_name, seg_file_name)

        """

    def reinitialize_db(self):

        return


    def initialize_db(self, dir_name, seg_file_name):

        self._closed_seg_table = OrderedDict()
        self._index_table = {}
        self._curr_seg_id = 0
        self._curr_seg_size = 0

        self._dir_name = dir_name
        if not os.path.isdir(dir_name):

            os.mkdir(dir_name)

        self.serialize_closed_seg_table()
        return


    def serialize_closed_seg_table(self):


        path = os.path.join(self._dir_name, '.closed_seg_indices')

        seg_file = open(path, 'w+')
        json.dump(self._closed_seg_table, seg_file)
        seg_file.close()
    

    def serialize_current_seg_table(self):

        path = os.path.join(self._dir_name, '.current_seg_indices')
        seg_file = open(path, 'w+')
        json.dump(self._index_table, seg_file)
        seg_file.close()


    def deserialize_closed_seg_table(self):


        path = os.path.join(self._dir_name, '.closed_seg_indices')
        seg_file = open(path, 'w+')
        closed_seg_raw = json.load(seg_file.read(), object_pairs_hook=OrderedDict)
        seg_file.close()
        self._closed_seg_table = closed_seg_raw


    def deserialize_current_seg_table(self):


        path = os.path.join(self._dir_name, '.current_seg_indices')
        seg_file = open(path, 'w+')
        closed_seg_raw = json.load(seg_file.read(), object_pairs_hook=OrderedDict)
        seg_file.close()
        self._closed_seg_table = closed_seg_raw


    def insert_row(self, key: int, val: dict):

        append_write = ''

        seg_name = self._seg_file_name + str(self._curr_seg_id)

        val_str = json.dumps(val)
        input_string = f'{key},{val_str}\n'

        seg_path = os.path.join(self._dir_name, 'segs', seg_name)
        
        if os.path.isfile(seg_path):

            offset = os.path.getsize(seg_path)
            
            if offset + len(input_string) > 4000:

                #close current segment
                
                self._closed_seg_table[self._curr_seg_id] = self._index_table
                self._index_table = {}
                self._curr_seg_id += 1
                seg_name = self._seg_file_name + str(self._curr_seg_id)

                self.serialize_closed_seg_table()
                append_write = 'w'
                offset = 0
            else:

                append_write = 'a'
        else:
            append_write = 'w'
            offset = 0
        
        self._index_table[key] = offset
        seg_path = os.path.join(self._dir_name, 'segs', seg_name)
        log_file = open(seg_path, append_write)
        log_file.write(input_string)
        log_file.close()


    def get_row(self, key: int):


        found = False
        if key in self._index_table:

            offset = self._index_table[key]
            seg_file_name = self._seg_file_name + str(self._curr_seg_id)

        else:

            for seg_id, index_table in reversed(self._closed_seg_table.items()):
                if key in index_table:
                    offset = index_table[key]
                    seg_file_name = self._seg_file_name + str(seg_id)
                    found = True
                    break
            
            if not found:
                print('Error: No such key value found in HashIndexDB')
                raise Exception

        seg_file = open(os.path.join(self._dir_name, 'segs', seg_file_name), 'r')
        seg_file.seek(offset)
        row = seg_file.readline()
        seg_file.close()

        value = json.loads(row.split(',', 1)[1])

        return value


    def compaction_and_merge(self):

        new_closed_seg_table = OrderedDict()
        new_id = 0
        new_seg_name = '.temp'+ str(new_id)
        overall_index_table = {}

        # Iterate through OrderedDict of closed seg tables in reverse, moving from most recently closed to least recently closed
        for seg_id, index_table in reversed(self._closed_seg_table.items()):

            seg_file = self._seg_file_name + str(seg_id)
            curr_seg_file = open(seg_file, 'r')

            # Iterate through key/offset map for current segment
            for key, old_offset in index_table.items():
                
                if key in overall_index_table:
                    continue

                curr_seg_file.seek(old_offset)
                line = curr_seg_file.readline()

                
                seg_path = os.path.join(self._dir_name, 'segs', new_seg_name)
                if os.path.isfile(seg_path):
                    
                    offset = os.path.getsize(seg_path)
                    if len(line) + offset > 4000:
                        
                        append_write = 'w'
                        new_id += 1
                        offset = 0
                    else:
                        append_write = 'a'
                    
                else:

                    append_write = 'w'
                    offset = 0

                if new_id not in new_closed_seg_table:
                    new_closed_seg_table[new_id] = {}
                
                new_seg_name = '.temp'+ str(new_id)
                overall_index_table[key] = offset
                new_closed_seg_table[new_id][key] = offset
                new_seg_file = open(seg_path, append_write)

                new_seg_file.write(line)
                new_seg_file.close()
            
            curr_seg_file.close()
        
        
        for i in range(self._curr_seg_id):

            del_file = self._seg_file_name + str(i)
            os.remove(del_file)
        
        count = 0
        for seg_id, table in reversed(new_closed_seg_table.items()):

            new_file_name = self._seg_file_name + str(count)
            old_file_name = '.temp' + str(seg_id)

            os.rename(old_file_name, new_file_name)


        self.serialize_closed_seg_table()




def hash_db_test():

    db = HashIndexDB()

    for i in range(100):

        for j in range(5):

            db.insert_row(i, {'name': f'Ryhan{j}'})

    for i in range(5):

        print(i, db.get_row(i))

    #db.compaction_and_merge()
    db.serialize_current_seg_table()

    #db.serialize_closed_seg_table()



if __name__ == '__main__':

    hash_db_test()