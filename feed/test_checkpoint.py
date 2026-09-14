import ast,datetime,json,os,pathlib,tempfile,unittest
source=pathlib.Path(__file__).with_name('benchmark.py').read_text();tree=ast.parse(source)
ns={'datetime':datetime,'json':json,'os':os}
exec(compile(ast.Module(body=[x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name in ['atomic_text','prepare_round']],type_ignores=[]),'<checkpoint>','exec'),ns)
class Checkpoints(unittest.TestCase):
 def test_preserves_interrupted_attempt(self):
  with tempfile.TemporaryDirectory() as td:
   p=pathlib.Path(td);r=p/'round-2';r.mkdir();(r/'node1.frames').write_bytes(b'partial evidence')
   current,done=ns['prepare_round'](p,2)
   self.assertFalse(done);self.assertEqual(list(current.iterdir()),[])
   saved=list((p/'interrupted-attempts').glob('round-2-*/node1.frames'))
   self.assertEqual(len(saved),1);self.assertEqual(saved[0].read_bytes(),b'partial evidence')
 def test_skips_complete_round(self):
  with tempfile.TemporaryDirectory() as td:
   p=pathlib.Path(td);r=p/'round-1';r.mkdir();(r/'pairs.jsonl').write_text('')
   ns['atomic_text'](r/'summary.json',json.dumps({'meta':{'round':1,'measurement_seconds':300}}))
   self.assertEqual(ns['prepare_round'](p,1),(r,True));self.assertFalse((p/'interrupted-attempts').exists())
 def test_rejects_wrong_window(self):
  with tempfile.TemporaryDirectory() as td:
   p=pathlib.Path(td);r=p/'round-1';r.mkdir();(r/'summary.json').write_text(json.dumps({'meta':{'round':1,'measurement_seconds':30}}))
   with self.assertRaises(RuntimeError):ns['prepare_round'](p,1)
 def test_rejects_missing_pair_data(self):
  with tempfile.TemporaryDirectory() as td:
   p=pathlib.Path(td);r=p/'round-1';r.mkdir();(r/'summary.json').write_text(json.dumps({'meta':{'round':1,'measurement_seconds':300}}))
   with self.assertRaises(RuntimeError):ns['prepare_round'](p,1)
if __name__=='__main__':unittest.main()
