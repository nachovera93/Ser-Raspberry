import serial
import time

def main():
    try:
        esp32 = serial.Serial('/dev/ttyUSB0', 230400, timeout=0.5)
        esp32.flushInput()
    except:
        esp32 = serial.Serial('/dev/ttyUSB2', 230400, timeout=0.5)
        esp32.flushInput()

    while True:
        # Enviar datos desde la Raspberry Pi al ESP32
        data_to_send = "Hello, ESP32!"
        esp32.write(data_to_send.encode())

        # Recibir datos desde el ESP32 a la Raspberry Pi
        data_received = esp32.readline().decode().strip()
        if data_received:
            print(f"Received from ESP32: {data_received}")

        time.sleep(1)

if __name__ == '__main__':
    main()
