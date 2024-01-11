import unittest
import random
from pathlib import Path
from hashlib import md5
from subprocess import run as runCmd
import psycopg2

import dbgen

SAFE_RUN_NUMBER = 100000

class TestGenerateAddressLen (unittest.TestCase):

    def testGenerateAddressLen(self):
        for i in range(SAFE_RUN_NUMBER):
            genAddress = dbgen.generateAddress()
            self.assertLessEqual(len(genAddress), 40)
            self.assertGreaterEqual(len(genAddress), 10)

class TestGeneratePhoneLen (unittest.TestCase):

    def testGeneratePhoneLen(self):
        for i in range(SAFE_RUN_NUMBER):
            genPhone = dbgen.generatePhone(random.randint(0, 25))
            self.assertEqual(len(genPhone), 15)

class TestGenerateCommentLen (unittest.TestCase):
    
    def testGenerateNCommentLen(self):
        for i in range(SAFE_RUN_NUMBER):
            genComment = dbgen.generateComment(152)
            self.assertLessEqual(len(genComment), 152)

    def testGenerateRCommentLen(self):
        for i in range(SAFE_RUN_NUMBER):
            genComment = dbgen.generateComment(152)
            self.assertLessEqual(len(genComment), 152)

    def testGeneratePCommentLen(self):
        for i in range(SAFE_RUN_NUMBER):
            genComment = dbgen.generateComment(23)
            self.assertLessEqual(len(genComment), 23)

    def testGenerateSCommentLen(self):
        for i in range(SAFE_RUN_NUMBER):
            genComment = dbgen.generateComment(101)
            self.assertLessEqual(len(genComment), 101)

    def testGeneratePSCommentLen(self):
        for i in range(SAFE_RUN_NUMBER):
            genComment = dbgen.generateComment(199)
            self.assertLessEqual(len(genComment), 199)

    def testGenerateCCommentLen(self):
        for i in range(SAFE_RUN_NUMBER):
            genComment = dbgen.generateComment(117)
            self.assertLessEqual(len(genComment), 117)

    def testGenerateOCommentLen(self):
        for i in range(SAFE_RUN_NUMBER):
            genComment = dbgen.generateComment(79)
            self.assertLessEqual(len(genComment), 79)

    def testGenerateLCommentLen(self):
        for i in range(SAFE_RUN_NUMBER):
            genComment = dbgen.generateComment(44)
            self.assertLessEqual(len(genComment), 44)

def calculateHash(path: Path) -> str:
        pathMd5 = md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                pathMd5.update(chunk)
        return pathMd5.hexdigest()

class TestSameResultsForTwoRuns (unittest.TestCase):

    # create two result directories 
    def setUp(self) -> None:
        dbgen.config.v1Only = False
        dbgen.config.updateTimes = 100000
        self.firstRunPath = Path.cwd() / "first-run"
        self.secondRunPath = Path.cwd() / "second-run"
        self.firstRunPath.mkdir()
        self.secondRunPath.mkdir()

    def testSameResultsForTwoRuns(self):
        connStr = "dbname={} user={} password={} host={} port={}".format(
            dbgen.config.dbname, 
            dbgen.config.user, 
            dbgen.config.password, 
            dbgen.config.host, 
            dbgen.config.port
        )
        
        dbgen.config.destPath = str(self.firstRunPath)
        dbgen.initializeVersion1(connStr, False)
        dbgen.generataHistory(connStr, False)

        # Reset random generators
        dbgen.UPDATE_P_RAND = random.Random(0.7579544029403025)
        dbgen.UPDATE_SCENARIO_P_RAND = random.Random(0.420571580830845)
        dbgen.UNIFORM_RAND = random.Random(0.25891675029296335)
        dbgen.TABLE_SAMPLE_SEED_RAND = random.Random(0.5112747213686085)

        dbgen.config.destPath = str(self.secondRunPath)
        dbgen.initializeVersion1(connStr, False)
        dbgen.generataHistory(connStr, False)

        # compare files
        commonFiles = set(self.firstRunPath.glob("*")) & set(self.secondRunPath.glob("*"))

        for file in commonFiles:
            print(file.name)
            firstRunFile = self.firstRunPath / file.name
            secondRunFile = self.secondRunPath / file.name
            self.assertEqual(
                calculateHash(firstRunFile), 
                calculateHash(secondRunFile)
            )

    # delete two result directories 
    def tearDown(self) -> None:
        for file in self.firstRunPath.glob("*"):
            file.unlink()
        for file in self.secondRunPath.glob("*"):
            file.unlink()
        self.firstRunPath.rmdir()
        self.secondRunPath.rmdir()
        dbgen.config.v1Only = True
        dbgen.config.updateTimes = 10000

class TestInvalidUpdateTimes(unittest.TestCase):

    def testInvalidUpdateTimes(self):
        dbgen.config.updateTimes = 0 
        with self.assertRaises(AssertionError):
            dbgen.config.config()

        dbgen.config.updateTimes = -1
        with self.assertRaises(AssertionError):
            dbgen.config.config()

        dbgen.config.updateTimes = 10000

class TestInvalidTpchTblPath(unittest.TestCase):
    def setUp(self) -> None:
        self.originPath = Path(dbgen.config.tpchTblPath)

    def testEmptyAndNonexistedTpchTblPath(self):
        dbgen.config.tpchTblPath = ""
        with self.assertRaises(AssertionError):
            dbgen.config.config()
        dbgen.config.tpchTblPath = str(self.originPath)

    def testDefaultIsInvalid(self):
        newPath = self.originPath.parent / "temp"
        if self.originPath.exists():
            self.originPath.rename(newPath)
        with self.assertRaises(AssertionError):
            dbgen.config.config()
        if newPath.exists():
            newPath.rename(self.originPath)

    def testAtLeastOneTableMissed(self):
        newPath = Path.cwd() / "temp"
        newPath.mkdir()
        ordersPath = newPath / "orders.tbl"
        ordersPath.touch()
        dbgen.config.tpchTblPath = str(newPath)
        with self.assertRaises(AssertionError):
            dbgen.config.config()
        ordersPath.unlink()
        newPath.rmdir()
        dbgen.config.tpchTblPath = str(self.originPath)

        
class TestInvalidPsqlPath(unittest.TestCase):

    def testInvalidPsqlPath(self):
        dbgen.config.psqlPath = ""
        with self.assertRaises(AssertionError):
            dbgen.config.config()
        
        dbgen.config.psqlPath = str(Path.cwd() / "psql")
        with self.assertRaises(FileNotFoundError):
            dbgen.config.config()

        dbgen.config.psqlPath = "psql"


class TestValidV1Only(unittest.TestCase):

    def testValidV1Only(self):
        dbgen.config.v1Only = 10
        with self.assertRaises(AssertionError):
            dbgen.config.config()
        dbgen.config.v1Only = True

if __name__ == "__main__":
    unittest.main()