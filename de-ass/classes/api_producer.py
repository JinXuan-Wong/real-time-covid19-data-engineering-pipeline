# Author: Wong Jin Xuan
# Task 1 – COVID-19 API -> Kafka producer (live / simulate / replay, class-based)

import json
import time
import random
import requests
from kafka import KafkaProducer


class CovidAPIProducer:
    def __init__(self, bootstrap_servers, topic, url,
                 poll_seconds=5, mode="live", replay_file="covid_data_log.json"):
        """
        mode: "live" (default), "simulate" (small random movements), or "replay"
        """
        self.topic = topic
        self.url = url
        self.poll_seconds = poll_seconds
        self.mode = mode
        self.replay_file = replay_file
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=200,
            acks="all",
        )

    def _clamp_nonneg(self, x):
        try:
            xi = int(x)
            return xi if xi > 0 else 0
        except Exception:
            return 0

    def _perturb_counts(self, rec,
                        case_mu=0.0008,
                        death_mu=0.00002,
                        test_mu=0.0012):
        """Add tiny deltas so numbers 'move' while keeping schema the same."""
        rec = dict(rec)  # copy
        pop = int(rec.get("population") or 0)
        cases = int(rec.get("cases") or 0)
        deaths = int(rec.get("deaths") or 0)
        tests = int(rec.get("tests") or 0)

        exp_cases = max(1, int(pop * case_mu / 1_000))
        exp_tests = max(1, int(pop * test_mu / 1_000))
        exp_deaths = max(0, int(exp_cases * death_mu / max(case_mu, 1e-9)))

        dc = random.randint(0, exp_cases)
        dd = random.randint(0, exp_deaths)
        dt = random.randint(0, exp_tests)

        rec["todayCases"] = self._clamp_nonneg(dc)
        rec["todayDeaths"] = self._clamp_nonneg(dd)
        rec["cases"] = self._clamp_nonneg(cases + dc)
        rec["deaths"] = self._clamp_nonneg(deaths + dd)
        rec["tests"] = self._clamp_nonneg(tests + dt)
        rec["updated"] = int(time.time() * 1000)
        return rec

    def _fetch_live(self):
        r = requests.get(self.url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            raise ValueError("Expected list of country records from API.")
        return data

    def _iter_replay(self):
        with open(self.replay_file, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    rec = json.loads(s)
                    if isinstance(rec, dict):
                        yield rec
                except json.JSONDecodeError:
                    continue

    def run(self):
        print(f"Producer started… topic={self.topic}, mode={self.mode}")
        try:
            if self.mode == "replay":
                count = 0
                for rec in self._iter_replay():
                    self.producer.send(self.topic, rec)
                    count += 1
                    time.sleep(0.02)
                    if count % 200 == 0:
                        self.producer.flush()
                        print(f"[replay] sent {count} records…")
                self.producer.flush()
                print(f"[replay] done. Total sent: {count}")
                return

            while True:
                try:
                    batch = self._fetch_live()
                except Exception as e:
                    print(f"[warn] fetch failed: {e}; retrying in {self.poll_seconds}s")
                    time.sleep(self.poll_seconds)
                    continue

                if self.mode == "simulate":
                    random.shuffle(batch)
                    batch = [self._perturb_counts(r) for r in batch]

                print(f"Fetched {len(batch)} records; sending…")
                for rec in batch:
                    self.producer.send(self.topic, rec)
                self.producer.flush()
                print(f"Batch sent: {len(batch)} records")

                time.sleep(self.poll_seconds)

        except KeyboardInterrupt:
            print("\nProducer stopped by user.")
        finally:
            self.producer.close()


if __name__ == "__main__":
    CovidAPIProducer(
        bootstrap_servers="localhost:9092",
        topic="covid19_stream",
        url="https://disease.sh/v3/covid-19/countries",
        poll_seconds=5,
        mode="simulate", 
    ).run()
