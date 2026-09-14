import unittest
from race import parse_response,request
class ProtocolTests(unittest.TestCase):
 def test_fragmented_length(self):
  wire=b'HTTP/1.1 202 Accepted\r\nContent-Length: 12\r\n\r\n{"result":1}'
  for i in range(len(wire)):self.assertIsNone(parse_response(wire[:i]))
  self.assertEqual(parse_response(wire)['rpc'],{'result':1})
 def test_chunked(self):
  wire=b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nc\r\n{"result":1}\r\n0\r\n\r\n'
  self.assertEqual(parse_response(wire)['rpc'],{'result':1})
 def test_auth_not_in_body(self):
  r=request('host','method',[], 'test-uuid');h,b=r.split(b'\r\n\r\n');self.assertIn(b'Authorization: Bearer test-uuid',h);self.assertNotIn(b'test-uuid',b)
if __name__=='__main__':unittest.main()
