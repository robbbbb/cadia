from utils import RateLimiter
from time import sleep

def test_ratelimiter():
    r=RateLimiter(1,1) # 1 per second
    assert r.inc() == True
    assert r.inc() == False
    sleep(1) # TODO use freezegun instead
    assert r.inc() == True
