import redis
from paho.mqtt import client as mqtt_client

broker = 'broker.emqx.io'
port = 1883
topic = "hospital/patients"

# Connect to Redis
r = redis.Redis(host='127.0.0.1', port=6379, db=0)


def get_next_doctor(doctors):
    # Get the current doctor index from Redis
    current_doctor = int(r.get('current_doctor') or 0)

    # Update the current doctor index in Redis
    r.set('current_doctor', (current_doctor + 1) % len(doctors))

    # Return the next doctor
    return doctors[current_doctor]


def assign_patient(patient, doctors):
    # Get the next doctor for the patient
    next_doctor = get_next_doctor(doctors)

    # Check if the patient has a assigned doctor
    if r.hexists(patient, 'doctor'):
        # Get the assigned doctor for the patient
        assigned_doctor = r.hget(patient, 'doctor').decode('utf-8')

        # Check if the assigned doctor is still on duty
        if assigned_doctor in doctors:
            # Assign the patient to the same doctor
            next_doctor = assigned_doctor

    # Assign the patient to the next doctor
    r.hset(patient, 'doctor', assigned_doctor)

    # publish to MQTT
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.publish(patient, assigned_doctor)
    print("data published")


def get_patient_info(patient):
    # Check if the patient exists in Redis
    if r.exists(patient):
        # Get the patient information
        doctor = r.hget(patient, 'doctor').decode('utf-8')
        return f'{patient} has been assigned to {doctor}'
    else:
        return f'{patient} has not been assigned to any doctor'


# Example usage
doctors = ['Dr. Smith', 'Dr. Johnson', 'Dr. Brown']
patient_1 = 'Patient 1'
patient_2 = 'Patient 2'
patient_3 = 'Patient 3'

assign_patient(patient_1, doctors)
print(get_patient_info(patient_1))

assign_patient(patient_2, doctors)
print(get_patient_info(patient_2))

assign_patient(patient_3, doctors)
print(get_patient_info(patient_3))

assign_patient(patient_1, doctors)
print(get_patient_info(patient_1))
