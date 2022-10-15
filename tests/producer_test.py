import sys
import uuid
sys.path.append('../src/')
import producer


def test_phnoGen():
    pInstance = producer.Producer(1,100)
    phno1 = pInstance.phnoGenerator("USA")
    phno2 = pInstance.phnoGenerator("USA")
    assert phno1!=phno2

def test_MessageLen():
    pInstance = producer.Producer(1,100)
    message = pInstance.messageGenerator()
    assert len(message) == 100

def test_uniqueID():
    id1 = str(uuid.uuid4())
    id2 = str(uuid.uuid4())
    assert id1!=id2


