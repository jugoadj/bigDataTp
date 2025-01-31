import datetime as dt
import json
import random
import uuid
from confluent_kafka import Producer, KafkaError
import logging

# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Fonction pour générer des transactions aléatoires
def generate_transaction():
    """
    Génère une transaction aléatoire avec des champs variés.
    """
    transaction_types = ['achat', 'remboursement', 'transfert']
    payment_methods = ['carte_de_credit', 'especes', 'virement_bancaire', 'erreur']
    current_time = dt.datetime.now().isoformat()

    villes = ["Paris", "Marseille", "Lyon", None]
    rues = ["Rue de la République", "Rue de Paris", None]

    transaction_data = {
        "id_transaction": str(uuid.uuid4()),
        "type_transaction": random.choice(transaction_types),
        "montant": round(random.uniform(10.0, 1000.0), 2),
        "devise": "USD",
        "date": current_time,
        "lieu": f"{random.choice(rues)}, {random.choice(villes)}",
        "moyen_paiement": random.choice(payment_methods),
        "details": {
            "produit": f"Produit{random.randint(1, 100)}",
            "quantite": random.randint(1, 10),
            "prix_unitaire": round(random.uniform(5.0, 200.0), 2)
        },
        "utilisateur": {
            "id_utilisateur": f"User{random.randint(1, 1000)}",
            "nom": f"Utilisateur{random.randint(1, 1000)}",
            "adresse": f"{random.randint(1, 1000)} {random.choice(rues)}, {random.choice(villes)}",
            "email": f"utilisateur{random.randint(1, 1000)}@example.com"
        }
    }
    return transaction_data


# Fonction de callback pour confirmer la livraison des messages
def delivery_report(err, msg):
    """
    Callback pour gérer les rapports de livraison des messages Kafka.
    """
    if err is not None:
        logger.error(f"Échec de la livraison du message : {err}")
    else:
        logger.info(f"Message livré à {msg.topic()} [{msg.partition()}]")


# Configuration Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',  # Adresse du broker Kafka
    'message.timeout.ms': 5000,  # Délai d'attente pour la livraison des messages
}

# Créer un producteur Kafka
producer = Producer(KAFKA_CONFIG)


def send_transactions(num_transactions=200):
    """
    Envoie un nombre spécifié de transactions au topic Kafka.
    """
    try:
        for _ in range(num_transactions):
            # Générer une transaction aléatoire
            transaction = generate_transaction()

            # Envoyer la transaction au topic Kafka
            producer.produce(
                topic='transaction',
                value=json.dumps(transaction).encode('utf-8'),
                callback=delivery_report
            )
            logger.info(f"Transaction générée : {transaction}")

        # Attendre que tous les messages soient livrés
        producer.flush()
        logger.info("Toutes les transactions ont été envoyées avec succès.")
    except KafkaError as e:
        logger.error(f"Erreur Kafka : {e}")
    except Exception as e:
        logger.error(f"Erreur inattendue : {e}")


# Point d'entrée du script
if __name__ == "__main__":
    send_transactions(num_transactions=200)