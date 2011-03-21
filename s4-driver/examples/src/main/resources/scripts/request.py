import io.s4.client.driver
import pprint;
import sys;

mode = {'readMode': 'private', 'writeMode': 'enabled'};

stream = sys.argv[1];
clazz = sys.argv[2];

d = io.s4.client.driver.Driver('localhost', 2334)
d.setDebug(True);

d.initialize();
d.connect(mode);

print "Sending all requests..."

for req in sys.stdin.readlines():
    d.send(stream, clazz, req);

print "Waiting 5 sec to collect all responses..."
responses = d.recvAll(5);
print "\n"*4

print "Done. Results:"
print pprint.pformat(responses, indent=4);

d.disconnect();
