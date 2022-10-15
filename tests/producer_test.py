import unittest
import producer

class TestProducer(unittest.TestCase):
    def testphnoGen(self):
        pInstance = producer.Producer(1,100)
        phno1 = pInstance.phnoGenerator("USA")
        phno2 = pInstance.phnoGenerator("USA")
        self.assertTrue(phno1!=phno2)

    def checkMessageLen(self):
        pInstance = producer.Producer(1,100)
        message = pInstance.messageGenerator(100)
        self.assertEquals(len(message), 100)

if __name__ == '__main__':
    unittest.main()


