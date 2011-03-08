import io.s4.client.driver
import sys;

if (len(sys.argv) > 1):
  mode = eval(sys.argv[1]);
else:
  mode = {'readMode': 'all', 'writeMode': 'disabled'};

d = io.s4.client.driver.Driver('localhost', 2334)
d.setDebug(True);

d.initialize();
d.connect(mode);

print (repr(d.recvAll(5)));

d.disconnect();
