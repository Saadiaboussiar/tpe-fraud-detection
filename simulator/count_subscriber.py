"""Abonne de test (MVP 1, etape 3) : compte les messages recus et les compare a ceux emis.

A lancer AVANT le simulateur (un message publie sans abonne est perdu).

  terminal 1 :  python simulator/count_subscriber.py
  terminal 2 :  python simulator/simulate_tpe.py

L'abonne s'arrete tout seul apres quelques secondes sans message (--idle), ecrit
data/sim/received.json et compare le total au champ "emitted" de data/sim/counts.json.
Code de sortie : 0 si recus == emis, 1 sinon.
"""
import argparse
import json
import os
import sys
import time
from collections import Counter

import paho.mqtt.client as mqtt

state = {"received": 0, "bad_json": 0, "by_terminal": Counter(), "last": None}


def on_connect(client, userdata, flags, rc, properties=None):
    print(f"connecte (code {rc}), abonnement a {userdata['topic']}")
    client.subscribe(userdata["topic"], qos=1)      # reabonne aussi apres une reconnexion


def on_message(client, userdata, msg):
    state["received"] += 1
    state["last"] = time.time()
    try:
        json.loads(msg.payload)
    except ValueError:
        state["bad_json"] += 1
    state["by_terminal"][msg.topic.split("/")[1]] += 1
    if state["received"] % 1000 == 0:
        print(f"  {state['received']} messages recus...")


def main():
    p = argparse.ArgumentParser(description="Compte les messages MQTT recus")
    p.add_argument("--host", default=os.getenv("MQTT_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    p.add_argument("--topic", default="tpe/+/transactions")
    p.add_argument("--idle", type=float, default=5.0, help="arret apres N secondes sans message")
    p.add_argument("--start-timeout", type=float, default=300.0, help="abandon si aucun message n'arrive")
    p.add_argument("--counts", default="data/sim/counts.json")
    p.add_argument("--out", default="data/sim/received.json")
    a = p.parse_args()

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="tpe-count-subscriber",
                             userdata={"topic": a.topic})
    except AttributeError:          # paho-mqtt 1.x
        client = mqtt.Client(client_id="tpe-count-subscriber", userdata={"topic": a.topic})
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(a.host, a.port, keepalive=60)
    except OSError as exc:
        sys.exit(f"Mosquitto injoignable sur {a.host}:{a.port} ({exc}). Lancez 'docker compose up -d'.")

    client.loop_start()
    t0 = time.time()
    print("en attente des messages (lancez maintenant le simulateur, Ctrl+C pour arreter)...")
    try:
        while True:
            time.sleep(0.5)
            now = time.time()
            if state["last"] is None and now - t0 > a.start_timeout:
                print("aucun message recu, abandon")
                break
            if state["last"] is not None and now - state["last"] > a.idle:
                break
    except KeyboardInterrupt:
        print("arret manuel")
    finally:
        client.loop_stop()
        client.disconnect()

    expected = None
    if os.path.exists(a.counts):
        with open(a.counts, encoding="utf-8") as f:
            c = json.load(f)
        expected = c.get("emitted", c.get("emitted_expected"))

    result = {"received": state["received"], "bad_json": state["bad_json"],
              "by_terminal": dict(sorted(state["by_terminal"].items())),
              "emitted": expected, "match": expected is not None and state["received"] == expected}
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"recus : {state['received']}   emis : {expected}   JSON invalides : {state['bad_json']}")
    if expected is None:
        print("counts.json introuvable : impossible de comparer")
        sys.exit(1)
    if state["received"] == expected:
        print("OK : recus = emis")
        sys.exit(0)
    print("ECART : recus != emis (voir les remarques du guide : abonne lance trop tard, "
          "ou livraison QoS 1 en double)")
    sys.exit(1)


if __name__ == "__main__":
    main()