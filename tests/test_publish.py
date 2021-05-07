import shutil
import unittest
from datetime import timedelta
from io import StringIO
from multiprocessing import Pool, cpu_count
from os import stat, path

import coverage
import ray
from electionguard.group import int_to_p_unchecked, int_to_q_unchecked
from electionguard.ballot import _list_eq
from electionguard.election import InternalElectionDescription
from electionguard.elgamal import ElGamalKeyPair
from electionguardtest.elgamal import elgamal_keypairs
from hypothesis import settings, given, HealthCheck, Phase, reproduce_failure
from hypothesis.strategies import booleans

from arlo_e2e.decrypt import (
    decrypt_ballots,
    verify_proven_ballot_proofs,
    exists_proven_ballot,
    write_proven_ballot,
    load_proven_ballot,
)
from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.publish import (
    load_fast_tally,
    load_ray_tally,
)
from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.ray_tally import ray_tally_everything
from arlo_e2e.tally import fast_tally_everything
from arlo_e2e_testing.dominion_hypothesis import dominion_cvrs

TALLY_TESTING_DIR = "tally_test"
DECRYPTED_DIR = "decrypted_test"


class TestTallyPublishing(unittest.TestCase):
    def removeTree(self) -> None:
        try:
            shutil.rmtree(TALLY_TESTING_DIR, ignore_errors=True)
            shutil.rmtree(DECRYPTED_DIR, ignore_errors=True)
        except FileNotFoundError:
            # okay if it's not there
            pass

    def setUp(self) -> None:
        self.removeTree()
        self.pool = Pool(cpu_count())
        ray_init_localhost()
        coverage.process_startup()  # necessary for coverage testing to work in parallel

    def tearDown(self) -> None:
        self.removeTree()
        self.pool.close()
        ray.shutdown()

    @given(dominion_cvrs(max_rows=50), booleans(), elgamal_keypairs())
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    # @reproduce_failure('6.1.1',
    #                    b'AXicPVRNbpNRDPS/Cy1CiE1EVIlVFy0LVArZULj/BTgHR2DG/kLUJvF7tsczni/6oKGlpmGaiu8ql7vndx2/707y9fO51B/bPj3K7an95eMH/fb0+rZaHt5U6tOPmw7/+UXs5fTL9WQZ38/3+nx+vZTdX97LLbqq8E9k3kTqiHAxN51zrhzAmIDTxhxSc+rd2VkRzjxFisUWMgzEbY7jymnIEn4iTX0C0SpLTKziE7sUSmacKXCXdtd9STQAiGO4nqwkdkdVMLuTR9LBSnM3W5QjeQLSqJ0E6aaFqmBUSXCvzBps1saQFvPS7BnJy3uYEADdRraIjIGatuANaBZxeZPA96wiE4KxZWVAO2LWMZmQDoQK13Y1fMhxU8OFCmFCARim9OlikZRSUOhelAqbKDE9JByxvQvzKHnHDsmJsd1uxlWsRoRG6dADMufOPYwdog/PzBGt93Ktg7EAORvqojawa46vTJIcgIVr44jibXbtyynlypDqKFddXEUOBLcxuOtJu5pVITyJrjuCl9RxtjypCSa7+u2fBPrPBxrnFOELsrI4RQQ47/pYYZTrWErBX6TIF+WyTLZGnc3pGBNp2bMl2GTatHoswQQdOFT5BNF/2Jkd0u4Kc2jqqtm5luNR1+gFPYEZa66pQI2PGGVl3TYMqczqSe/WIRZEArU4DMu9jlBQuvmva9t9xIGEGDvkU3z9ZWgayEdZPO5gI00cYEdrB606HPnExXVEkG1bu6r++XuT/wDEvBGl')
    def test_end_to_end_publications(
        self, input: str, check_proofs: bool, keypair: ElGamalKeyPair
    ) -> None:
        self.singleton_test_end_to_end_publications(input, check_proofs, keypair)

    def test_end_to_end_publications_known_failure(self) -> None:
        weird_data = '"Random Test Election",="5.2.16.1","","","","","","","","","","","","","","","","","","","","","","","","","",""\n"","","","","","","","","Contest1 (Vote For=1)","Contest1 (Vote For=1)","Contest2 (Vote For=1)","Contest2 (Vote For=1)","Contest2 (Vote For=1)","Contest2 (Vote For=1)","Contest2 (Vote For=1)","Contest3 (Vote For=1)","Contest3 (Vote For=1)","Contest4 (Vote For=1)","Contest4 (Vote For=1)","Contest4 (Vote For=1)","Contest4 (Vote For=1)","Contest4 (Vote For=1)","Contest5 (Vote For=1)","Contest5 (Vote For=1)","Referendum1","Referendum1","Referendum2","Referendum2"\n"","","","","","","","","Richard WHITE","Richard WALKER","Camille YOUNG","Patricia GUPTA","Anthony GUPTA","Barbara WALKER","Karen GARCIA","David MILLER","Sigurður TAYLOR","William WILSON","Patricia SMITH","Matthew JONES","Matthew WILLIAMS","Betty KING","Betty MILLER","Dorothy MARTIN","FOR","AGAINST","FOR","AGAINST"\n"CvrNumber","TabulatorNum","BatchId","RecordId","ImprintedId","CountingGroup","PrecinctPortion","BallotType","PARTY5","PARTY1","PARTY2","PARTY3","PARTY1","PARTY4","PARTY2","PARTY1","PARTY2","PARTY5","PARTY3","PARTY2","PARTY5","PARTY2","PARTY3","PARTY1","","","",""\n="1",="1",="2",="0",="1-1-1","Regular",="0","BALLOTSTYLE2",="0",="0",="1",="0",="0",="0",="0",="0",="0",="0",="0",="0",="1",="0","","","","",="1",="0"\n="2",="2",="0",="2",="1-1-2","In Person",="1","BALLOTSTYLE1",="1",="0","","","","","",="0",="1","","","","","","","","","",="1",="0"\n="3",="1",="2",="0",="1-1-3","Mail",="3","BALLOTSTYLE4","","",="0",="0",="1",="0",="0","","","","","","","",="0",="1",="0",="0",="0",="1"\n="4",="2",="0",="2",="1-1-4","Provisional",="3","BALLOTSTYLE0",="0",="0","","","","","",="1",="0",="0",="1",="0",="0",="0","","",="0",="0","",""\n="5",="0",="3",="0",="1-1-5","Mail",="3","BALLOTSTYLE1",="0",="0","","","","","",="1",="0","","","","","","","","","",="0",="1"\n="6",="3",="3",="0",="1-1-6","Election Day",="3","BALLOTSTYLE0",="0",="1","","","","","",="0",="1",="0",="0",="0",="1",="0","","",="0",="0","",""\n="7",="2",="1",="0",="1-1-7","Provisional",="3","BALLOTSTYLE2",="0",="0",="0",="1",="0",="0",="0",="0",="1",="0",="0",="0",="0",="0","","","","",="0",="0"\n="8",="2",="3",="3",="1-1-8","Mail",="2","BALLOTSTYLE3",="0",="0",="0",="0",="1",="0",="0",="0",="0","","","","","",="0",="1",="0",="1","",""\n="9",="0",="0",="0",="1-1-9","Regular",="0","BALLOTSTYLE2",="0",="1",="0",="1",="0",="0",="0",="0",="1",="0",="0",="1",="0",="0","","","","",="0",="1"\n="10",="0",="0",="1",="1-1-10","In Person",="2","BALLOTSTYLE2",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0","","","","",="0",="0"\n="11",="2",="3",="1",="1-1-11","Provisional",="2","BALLOTSTYLE3",="0",="1",="0",="0",="0",="0",="0",="0",="0","","","","","",="1",="0",="0",="1","",""\n="12",="1",="2",="3",="1-1-12","Election Day",="2","BALLOTSTYLE4","","",="0",="0",="0",="0",="0","","","","","","","",="0",="1",="0",="0",="0",="1"\n="13",="3",="1",="1",="1-1-13","In Person",="1","BALLOTSTYLE3",="0",="1",="0",="0",="0",="0",="0",="0",="1","","","","","",="0",="1",="0",="0","",""\n="14",="2",="1",="1",="1-1-14","In Person",="3","BALLOTSTYLE4","","",="0",="0",="0",="1",="0","","","","","","","",="0",="1",="0",="1",="0",="1"\n="15",="3",="3",="2",="1-1-15","Mail",="3","BALLOTSTYLE2",="0",="0",="0",="0",="0",="0",="0",="0",="1",="0",="0",="0",="0",="0","","","","",="0",="0"\n="16",="2",="3",="3",="1-1-16","Election Day",="3","BALLOTSTYLE4","","",="0",="0",="0",="1",="0","","","","","","","",="0",="0",="1",="0",="1",="0"\n="17",="0",="2",="3",="1-1-17","Election Day",="2","BALLOTSTYLE2",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0","","","","",="0",="0"\n="18",="2",="3",="0",="1-1-18","Election Day",="0","BALLOTSTYLE4","","",="0",="0",="0",="0",="0","","","","","","","",="0",="1",="0",="0",="1",="0"\n="19",="3",="0",="2",="1-1-19","In Person",="2","BALLOTSTYLE3",="0",="1",="0",="0",="0",="0",="0",="0",="0","","","","","",="1",="0",="1",="0","",""\n="20",="2",="3",="1",="1-1-20","Regular",="3","BALLOTSTYLE2",="0",="1",="0",="0",="0",="0",="1",="1",="0",="0",="0",="1",="0",="0","","","","",="1",="0"\n="21",="0",="3",="1",="1-1-21","Mail",="0","BALLOTSTYLE4","","",="0",="0",="0",="0",="0","","","","","","","",="0",="1",="0",="0",="0",="1"\n="22",="2",="0",="1",="1-1-22","Regular",="0","BALLOTSTYLE0",="0",="1","","","","","",="1",="0",="0",="0",="0",="0",="0","","",="0",="1","",""\n="23",="1",="3",="0",="1-1-23","In Person",="2","BALLOTSTYLE1",="0",="1","","","","","",="1",="0","","","","","","","","","",="0",="1"\n="24",="2",="1",="1",="1-1-24","Regular",="1","BALLOTSTYLE2",="1",="0",="0",="0",="1",="0",="0",="0",="1",="0",="0",="0",="0",="0","","","","",="0",="0"\n="25",="1",="0",="0",="1-1-25","Provisional",="3","BALLOTSTYLE3",="1",="0",="1",="0",="0",="0",="0",="0",="0","","","","","",="0",="0",="0",="0","",""\n="26",="2",="0",="1",="1-1-26","Provisional",="1","BALLOTSTYLE0",="0",="0","","","","","",="0",="0",="0",="0",="1",="0",="0","","",="1",="0","",""\n="27",="3",="2",="2",="1-1-27","Mail",="0","BALLOTSTYLE4","","",="0",="0",="0",="0",="1","","","","","","","",="1",="0",="0",="1",="0",="0"\n="28",="2",="3",="2",="1-1-28","Regular",="1","BALLOTSTYLE0",="0",="1","","","","","",="0",="1",="0",="0",="0",="0",="0","","",="0",="0","",""\n="29",="0",="1",="2",="1-1-29","Mail",="0","BALLOTSTYLE4","","",="0",="0",="0",="0",="0","","","","","","","",="0",="0",="0",="1",="0",="0"\n="30",="3",="3",="2",="1-1-30","Provisional",="1","BALLOTSTYLE2",="0",="0",="1",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0","","","","",="0",="0"\n="31",="2",="2",="2",="1-1-31","In Person",="2","BALLOTSTYLE1",="0",="0","","","","","",="0",="0","","","","","","","","","",="0",="0"\n="32",="0",="2",="2",="1-1-32","In Person",="2","BALLOTSTYLE0",="1",="0","","","","","",="1",="0",="0",="1",="0",="0",="0","","",="0",="0","",""\n="33",="2",="1",="0",="1-1-33","Mail",="2","BALLOTSTYLE4","","",="1",="0",="0",="0",="0","","","","","","","",="1",="0",="0",="1",="0",="0"\n="34",="2",="2",="2",="1-1-34","Election Day",="1","BALLOTSTYLE2",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="1","","","","",="0",="1"\n="35",="0",="2",="0",="1-1-35","Provisional",="0","BALLOTSTYLE0",="1",="0","","","","","",="1",="0",="0",="0",="0",="0",="0","","",="0",="0","",""\n="36",="2",="3",="0",="1-1-36","Provisional",="1","BALLOTSTYLE3",="0",="1",="0",="0",="0",="1",="0",="0",="0","","","","","",="0",="1",="0",="0","",""\n="37",="2",="0",="2",="1-1-37","Provisional",="2","BALLOTSTYLE4","","",="0",="0",="0",="0",="0","","","","","","","",="1",="0",="1",="0",="0",="1"\n="38",="0",="2",="3",="1-1-38","Election Day",="3","BALLOTSTYLE2",="1",="0",="0",="1",="0",="0",="0",="1",="0",="0",="0",="0",="0",="0","","","","",="0",="1"\n="39",="0",="3",="2",="1-1-39","In Person",="0","BALLOTSTYLE0",="0",="1","","","","","",="0",="1",="0",="0",="0",="0",="0","","",="1",="0","",""'
        keypair = ElGamalKeyPair(
            int_to_q_unchecked(3606054918),
            int_to_p_unchecked(
                1035617172329231086376286796176207355613289349526779966673886037049377122388504578201516399804296668710803683183566869490132703437726507432347691426247957907658744841667055290668583448984414437401442669612993678757930376742555611299137619358513137668647038606709804346556901348427088766041306103468819565535759736920269342663383728593749469306636477100582347550519475175708593979006027032146291254554613084505389346258516037384152147300568223017879470022686587150010317247679384099681662518629546807592987463773795480413782386316899767347652147759608998967138932927209978984970571267358256450116203141078959408770669151727297462824583851534021997140500747613297059214440859601948626467856764145440479890274888220793377007301100109802735825800639848888416967613360095355645667645651809636333406911572793347452841467994078927493124439708996704418680233704863202592187563944602418214591459091354195907179400526352757934974014932481514342085790156417292544646817044129322864041545362383299929424152582645629496382734708408512716584439818304407472975376621530907267833649364612227188070442036630187142653495716189833727123440240515440723932699172038568284999729075111312875813227955729819021872800851695062006296704686840017650949363949821
            ),
        )
        self.singleton_test_end_to_end_publications(
            input=weird_data, check_proofs=True, keypair=keypair
        )

    def singleton_test_end_to_end_publications(
        self, input: str, check_proofs: bool, keypair: ElGamalKeyPair
    ) -> None:
        coverage.process_startup()  # necessary for coverage testing to work in parallel
        self.removeTree()  # if there's anything leftover from a prior run, get rid of it

        cvrs = read_dominion_csv(StringIO(input))
        self.assertIsNotNone(cvrs)

        _, ballots, _ = cvrs.to_election_description()
        assert len(ballots) > 0, "can't have zero ballots!"

        results = fast_tally_everything(
            cvrs,
            self.pool,
            secret_key=keypair.secret_key,
            verbose=True,
            root_dir=TALLY_TESTING_DIR,
        )

        self.assertTrue(results.all_proofs_valid(self.pool))

        log_and_print("tally_testing written, proceeding to read it back in again")

        # now, read it back again!
        results2 = load_fast_tally(
            results_dir=TALLY_TESTING_DIR,
            check_proofs=check_proofs,
            pool=self.pool,
            verbose=True,
            recheck_ballots_and_tallies=True,
        )
        self.assertIsNotNone(results2)

        log_and_print("tally_testing got non-null result!")

        self.assertTrue(_list_eq(results.encrypted_ballots, results2.encrypted_ballots))
        self.assertTrue(results.equivalent(results2, keypair, self.pool))

        # Make sure there's an index.html file; throws an exception if it's missing
        self.assertIsNotNone(stat(path.join(TALLY_TESTING_DIR, "index.html")))

        # And lastly, while we're here, we'll use all this machinery to exercise the ballot decryption
        # read/write facilities.

        ied = InternalElectionDescription(results.election_description)

        log_and_print("decrypting one more time")
        pballots = decrypt_ballots(
            ied,
            results.context.crypto_extended_base_hash,
            keypair,
            self.pool,
            results.encrypted_ballots,
        )
        self.assertEqual(len(pballots), len(results.encrypted_ballots))
        self.assertNotIn(None, pballots)

        # for speed, we're only going to do this for the first ballot, not all of them
        pballot = pballots[0]
        eballot = results.encrypted_ballots[0]
        bid = pballot.ballot.object_id
        self.assertTrue(
            verify_proven_ballot_proofs(
                results.context.crypto_extended_base_hash,
                keypair.public_key,
                eballot,
                pballot,
            )
        )
        write_proven_ballot(pballot, DECRYPTED_DIR)
        self.assertTrue(exists_proven_ballot(bid, DECRYPTED_DIR))
        self.assertFalse(exists_proven_ballot(bid + "0", DECRYPTED_DIR))
        self.assertEqual(pballot, load_proven_ballot(bid, DECRYPTED_DIR))

        self.removeTree()  # clean up our mess

    @given(dominion_cvrs(max_rows=50), booleans(), elgamal_keypairs())
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_end_to_end_publications_ray(
        self, input: str, check_proofs: bool, keypair: ElGamalKeyPair
    ) -> None:
        self.removeTree()  # if there's anything leftover from a prior run, get rid of it

        cvrs = read_dominion_csv(StringIO(input))
        self.assertIsNotNone(cvrs)

        _, ballots, _ = cvrs.to_election_description()
        assert len(ballots) > 0, "can't have zero ballots!"

        results = ray_tally_everything(
            cvrs,
            secret_key=keypair.secret_key,
            verbose=True,
            root_dir=TALLY_TESTING_DIR,
        )

        self.assertTrue(results.all_proofs_valid())

        # dump files out to disk
        log_and_print("tally_testing written, proceeding to read it back in again")

        # now, read it back again!
        results2 = load_ray_tally(
            TALLY_TESTING_DIR,
            check_proofs=check_proofs,
            verbose=True,
            recheck_ballots_and_tallies=True,
        )
        self.assertIsNotNone(results2)

        log_and_print("tally_testing got non-null result!")

        self.assertTrue(_list_eq(results.encrypted_ballots, results2.encrypted_ballots))
        self.assertTrue(results.equivalent(results2, keypair))
        self.removeTree()  # clean up our mess
