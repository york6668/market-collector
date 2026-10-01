import json
from pathlib import Path
import tempfile
import unittest

from roles.export_role import ROOT, export
from roles import role_support


class CollectorExportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'repo'
        (self.root / 'roles').mkdir(parents=True)
        (self.root / 'shared.py').write_text('VALUE = 1\n')
        (self.root / 'private.json').write_text('{"secret":"not exported"}')
        self.destination = Path(self.temp.name) / 'export'

    def manifest(self, paths):
        (self.root / 'roles/collector-source-files.json').write_text(json.dumps(paths))

    def test_exports_only_listed_sources_and_does_not_overwrite(self):
        self.manifest(['shared.py'])
        result = export('collector', self.destination, self.root)
        self.assertEqual(1, result['files'])
        self.assertEqual('VALUE = 1\n', (self.destination / 'shared.py').read_text())
        self.assertFalse((self.destination / 'private.json').exists())
        with self.assertRaises(FileExistsError):
            export('collector', self.destination, self.root)

    def test_invalid_missing_duplicate_and_symlink_sources_leave_no_export(self):
        for paths in (['../escape.py'], ['/tmp/escape.py'], ['missing.py'],
                      ['shared.py', 'shared.py'], [], {'shared.py': 'shared.py'}):
            self.manifest(paths)
            with self.assertRaises(ValueError):
                export('collector', self.destination, self.root)
            self.assertFalse(self.destination.exists())
        (self.root / 'alias.py').symlink_to('shared.py')
        self.manifest(['alias.py'])
        with self.assertRaises(ValueError):
            export('collector', self.destination, self.root)

    def test_other_role_is_rejected_without_reading_or_creating_paths(self):
        with self.assertRaisesRegex(ValueError, 'only the collector'):
            export('analysis', self.destination, self.root)
        self.assertFalse(self.destination.exists())

    def test_real_export_can_be_reexported_and_packaged_without_original_checkout(self):
        paths = json.loads((ROOT / 'roles/collector-source-files.json').read_text())
        self.assertFalse(any(p.startswith(('analysis/', '.codex/', '.agents/')) for p in paths))
        first = export('collector', self.destination)
        second_path = Path(self.temp.name) / 'second'
        second = export('collector', second_path, self.destination)
        self.assertEqual(first['files'], second['files'])
        required = {'btc_push.py', 'btc_evidence.py', 'btc_evidence_reader.py',
                    'market_evidence_v2.py', 'collector_v2_reader.py', 'market_reader.py',
                    'requirements.txt', 'roles/validate_repository.py', 'roles/export_role.py'}
        self.assertTrue(required.issubset(paths))
        for name in paths:
            self.assertEqual((ROOT / name).read_bytes(), (second_path / name).read_bytes())
        bundle = Path(self.temp.name) / 'collector-reference.json'
        role_support.package(second_path, paths, bundle, 'collector')
        unpacked = Path(self.temp.name) / 'unpacked'
        role_support.unpack(bundle, unpacked)
        for name in paths:
            self.assertEqual((second_path / name).read_bytes(), (unpacked / name).read_bytes())


if __name__ == '__main__':
    unittest.main()
