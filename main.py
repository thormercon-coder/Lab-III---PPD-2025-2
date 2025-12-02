
import paho.mqtt.client as mqtt
import json
import random
import hashlib
import threading
import time

# -------------------- Configurações --------------------
BROKER = "f2622005.ala.us-east-1.emqxsl.com"
PORT = 8883
USERNAME = "broker1"
PASSWORD = "ISSOAQUIÉFLAMENGO"
N_PARTICIPANTS = 3
CLIENT_ID = str(random.randint(0, 65535))

TIMEOUT_INIT = 10     # Timeout para fase Init (segundos)
TIMEOUT_ELECTION = 10  # Timeout para fase Election (segundos)

transactions = []
init_msgs = {}  # {ClientID: True}
votes = {}      # {ClientID: VoteID}
is_leader = False

# -------------------- Funções Auxiliares --------------------
def publish(topic, data):
    payload = json.dumps(data)
    client.publish(topic, payload)

def sha1_hash(s):
    return hashlib.sha1(s.encode()).hexdigest()

# -------------------- Fases --------------------
def init_phase():
    print(f"[INIT] Meu ClientID = {CLIENT_ID}")
    publish("sd/init", {"ClientID": CLIENT_ID})
    start_time = time.time()

    while len(init_msgs) < N_PARTICIPANTS and (time.time() - start_time) < TIMEOUT_INIT:
        time.sleep(1)

    if len(init_msgs) < N_PARTICIPANTS:
        print(f"[TIMEOUT] Fase INIT expirou após {TIMEOUT_INIT}s. Recebidos: {len(init_msgs)} de {N_PARTICIPANTS}")
    else:
        print("\n===== FASE INIT COMPLETA =====")
        print(f"[INFO] Todos os participantes registrados.")
        print(f"[INFO] Lista de ClientIDs: {list(init_msgs.keys())}")
        print("================================\n")

    election_phase()

def election_phase():
    vote_id = random.randint(0, 65535)
    votes[CLIENT_ID] = vote_id
    publish("sd/election", {"ClientID": CLIENT_ID, "VoteID": vote_id})
    print(f"[ELECTION] Meu VoteID = {vote_id}")

    start_time = time.time()
    while len(votes) < N_PARTICIPANTS and (time.time() - start_time) < TIMEOUT_ELECTION:
        time.sleep(1)

    if len(votes) < N_PARTICIPANTS:
        print(f"[TIMEOUT] Fase ELECTION expirou após {TIMEOUT_ELECTION}s. Recebidos: {len(votes)} de {N_PARTICIPANTS}")

    # Decidir líder mesmo com dados incompletos
    leader = max(votes.items(), key=lambda x: (x[1], int(x[0])))[0]
    global is_leader
    is_leader = (leader == CLIENT_ID)

    print("\n===== RESULTADO DA ELEIÇÃO =====")
    print("[INFO] Lista de ClientIDs:")
    for cid in init_msgs.keys():
        print(f" - ClientID: {cid}")
    print("\n[INFO] Lista de VoteIDs:")
    for cid, vid in votes.items():
        print(f" - ClientID: {cid}, VoteID: {vid}")
    print(f"\n[INFO] Líder eleito: {leader}")
    if is_leader:
        print("[INFO] Este nó é o LÍDER.")
    else:
        print("[INFO] Este nó é um MINERADOR.")
    mineradores = [cid for cid in votes.keys() if cid != leader]
    print(f"[INFO] Mineradores: {mineradores}")
    print("================================\n")

    time.sleep(3)  # tempo extra para sincronização
    challenge_phase()

def challenge_phase():
    if is_leader:
        print("[CHALLENGE] Sou líder. Gerando desafio...")
        transaction_id = 0
        challenge = random.randint(1, 5)
        transactions.append({"TransactionID": transaction_id,
                            "Challenge": challenge, "Solution": "", "Winner": -1})
        publish("sd/challenge",
                {"TransactionID": transaction_id, "Challenge": challenge})
    else:
        print("[CHALLENGE] Aguardando desafio do líder...")

def solve_challenge(transaction_id, challenge):
    prefix = "0" * challenge
    attempt = 0
    while True:
        candidate = f"{CLIENT_ID}-{attempt}"
        hash_val = sha1_hash(candidate)
        if hash_val.startswith(prefix):
            return candidate
        attempt += 1

def validate_solution(transaction_id, solution, client_id):
    prefix = "0" * transactions[transaction_id]["Challenge"]
    hash_val = sha1_hash(solution)
    if hash_val.startswith(prefix):
        transactions[transaction_id]["Solution"] = solution
        transactions[transaction_id]["Winner"] = client_id
        return True
    return False

# -------------------- Callbacks --------------------
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Conectado ao broker com código {rc}")
    client.subscribe("sd/init")
    client.subscribe("sd/election")
    client.subscribe("sd/challenge")
    client.subscribe("sd/solution")
    client.subscribe("sd/result")

def on_message(client, userdata, message):
    data = json.loads(message.payload.decode())
    topic = message.topic

    if topic == "sd/init":
        init_msgs[data["ClientID"]] = True
    elif topic == "sd/election":
        votes[data["ClientID"]] = data["VoteID"]
    elif topic == "sd/challenge":
        print(f"[CHALLENGE] Recebido desafio: {data}")
        transaction_id = data["TransactionID"]
        challenge = data["Challenge"]
        transactions.append({"TransactionID": transaction_id,
                            "Challenge": challenge, "Solution": "", "Winner": -1})
        threading.Thread(target=miner_task, args=(
            transaction_id, challenge)).start()
    elif topic == "sd/solution" and is_leader:
        valid = validate_solution(
            data["TransactionID"], data["Solution"], data["ClientID"])
        result = 1 if valid else 0
        publish("sd/result", {"ClientID": data["ClientID"],
                "TransactionID": data["TransactionID"], "Solution": data["Solution"], "Result": result})
    elif topic == "sd/result":
        print(f"[RESULT] {data}")

def miner_task(transaction_id, challenge):
    print(
        f"[MINER] Resolvendo desafio TransactionID={transaction_id}, dificuldade={challenge}")
    solution = solve_challenge(transaction_id, challenge)
    print(f"[MINER] Solução encontrada: {solution}")
    publish("sd/solution", {"ClientID": CLIENT_ID,
            "TransactionID": transaction_id, "Solution": solution})

# -------------------- Main --------------------
client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311,
                     transport="tcp", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.tls_set()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, 60)
client.loop_start()

threading.Thread(target=init_phase).start()

while True:
    time.sleep(1)
