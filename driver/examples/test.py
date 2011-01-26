import io.s4.client.driver

d = io.s4.client.driver.Driver('localhost', 2334)
d.setDebug(True);

d.initialize();
d.connect({'readdMode': 'all'});

print (repr(d.recv()));
d.disconnect();
