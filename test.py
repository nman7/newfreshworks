import unittest
import datafile
import threading
import time

class TestDataStore(unittest.TestCase) :
    
    @classmethod
    def setUpClass(cls):
        cls.data = datafile.Dataclass('datastore') 

    def test_create(self) :
        print('test_create')

        
        self.assertEqual(
            self.data.create(1, {'name':'abc'}), 
            '1'
        )
        self.assertEqual(
            self.data.create(1, {'name':'xyz'}), 
            '1'
        )
        self.assertEqual(
            self.data.create(2, {'name':'pqr'}), 
            '2'
        )

    def test_read(self) :
        print('test_read')


        self.assertRaises(
            Exception, 
            self.data.read, 
            8
        )

        self.assertEqual(
            self.data.read(1),
            {'name':'xyz'}
        )

        time.sleep(20)

        self.assertRaises(
            Exception, 
            self.data.read, 
            1
        )



    def test_delete(self) :
        print('test_delete')

        self.assertRaises(
            Exception, 
            self.data.delete, 
            9
        )

        self.assertEqual(
            self.data.delete(2),
            '2'
        )


        time.sleep(10)

        self.assertRaises(
            Exception, 
            self.data.delete, 
            2
        )


    def test_threading(self) :

        t1 = threading.Thread(target = self.data.create, args=(12, {'name':'lop'}))
        t2 = threading.Thread(target = self.data.create, args=(21, {'name':'mnn'}))
        t3 = threading.Thread(target = self.data.create, args=(39, {'name':'eez'}))

        t1.start()
        t2.start()
        t3.start()



if __name__ == '__main__' :
    unittest.main()