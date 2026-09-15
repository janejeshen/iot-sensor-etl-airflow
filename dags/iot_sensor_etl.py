from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

@dag(
    dag_id="iot_sensor_etl",
    schedule="@daily")

def iot_sensor_etl():

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    @task()
    def extract_readings():
        sensors_readings = [
            {"sensor_id": 1,"timestamp": "2023-01-01T00:00:00Z", "temperature": 22.5, "humidity": 60.0},
            {"sensor_id": 2,"timestamp": "2023-01-01T00:00:00Z", "temperature": 23.0, "humidity": 55.0},
            {"sensor_id": 3,"timestamp": "2023-01-01T00:00:00Z", "temperature": 21.5, "humidity": 65.0},
            {"sensor_id": 4,"timestamp": "2023-01-01T00:00:00Z", "temperature": 24.0, "humidity": 50.0},
            {"sensor_id": 5,"timestamp": "2023-01-01T00:00:00Z", "temperature": 22.0, "humidity": 70.0},
            {"sensor_id": 6,"timestamp": "2023-01-01T00:00:00Z", "temperature": 23.5, "humidity": 55.0},
            {"sensor_id": 7,"timestamp": "2023-01-01T00:00:00Z", "temperature": 21.0, "humidity": 65.0},
            {"sensor_id": 8,"timestamp": "2023-01-01T00:00:00Z", "temperature": 24.5, "humidity": 50.0},
            {"sensor_id": 9,"timestamp": "2023-01-01T00:00:00Z", "temperature": 52.0, "humidity": 60.0},
            {"sensor_id": 10,"timestamp": "2023-01-01T00:00:00Z", "temperature": 23.0, "humidity": 105.0}
        ]

        print("Extracted sensor readings:", len(sensors_readings))
        print("Sample sensor reading:", sensors_readings[0])
        return sensors_readings

    @task()
    def transform_readings(readings):

        transformed_readings = []

        for reading in readings:
            transformed_reading = {
                "sensor_id": reading["sensor_id"],
                "timestamp": reading["timestamp"],
                "temperature_celsius": reading["temperature"],
                "temperature_fahrenheit": reading["temperature"] * 9/5 + 32,
                "humidity": reading["humidity"]}
            
            transformed_readings.append(transformed_reading)

        print("Transformed sensor readings:", len(transformed_readings))
        return transformed_readings

    @task()
    def validate_readings(readings):
        valid_readings = []
        invalid_readings = []

        for reading in readings:
            if 0 <= reading["temperature_celsius"] <= 50 and 0<= reading["humidity"] <= 100:
                valid_readings.append(reading)
            else:
                invalid_readings.append(reading)

        print("valid readings:", len(valid_readings))
        print("invalid readings:", len(invalid_readings))


        for reading in invalid_readings:
            print(f"Invalid reading - Sensor ID: {reading['sensor_id']}, Temperature: {reading['temperature_celsius']}, Humidity: {reading['humidity']}")

        return valid_readings

    @task()
    def load_readings(valid_readings):
        print(f"loading {len(valid_readings)} valid readings into the simulated data warehouse....")

        for reading in valid_readings:
            print(f"""Loading reading - Sensor ID: {reading['sensor_id']} |,
                   timestamp: {reading['timestamp']}|,
                   Temperature (C): {reading['temperature_celsius']}|, 
                   Temperature (F): {reading['temperature_fahrenheit']}|, 
                   Humidity: {reading['humidity']}%""")

    @task()
    def summarize_readings(readings):
        if not readings:
            print("No valid readings to summarize..")
        else:
            total_valid_readings = len(readings)
            avg_temperature_celsius = sum(reading["temperature_celsius"] for reading in readings) / total_valid_readings
            avg_humidity = sum(reading["humidity"] for reading in readings) / total_valid_readings
            print(f"Summary - Total Valid Readings: {total_valid_readings}")
            print(f"Average Temperature (C): {avg_temperature_celsius:.2f}")
            print(f"Average Humidity: {avg_humidity:.2f}%")


    end =EmptyOperator(task_id = "end_pipeline")


    raw_readings = extract_readings()
    transformed_readings = transform_readings(raw_readings)
    valid_readings = validate_readings(transformed_readings)
    load_task = load_readings(valid_readings)
    summarize_task = summarize_readings(valid_readings)

    start_pipeline >> raw_readings >> transformed_readings >> valid_readings >> load_task >> summarize_task >> end

iot_sensor_etl_dag = iot_sensor_etl()


    