import struct
import json
import time

class FlowMeterAccurateDecoder:
    def __init__(self):
        # Precise Mapping from Protocol Manual (Address Code * 2 = Byte Offset)
        self.mapping = [
            (0x00, 'float', 'transient_flow'),              # Instantaneous flow
            (0x02, 'long',  'total_cumulative_whole'),       # Positive integer part
            (0x04, 'float', 'total_cumulative_decimal'),     # Positive decimal part
            (0x06, 'long',  'negative_cumulative_whole'),    # Negative integer part
            (0x08, 'float', 'negative_cumulative_decimal'),  # Negative decimal part
            (0x0D, 'int',   'sampling_value'),               # Sensor sampling
            (0x0E, 'int',   'zero_point_sample'),            # Zero point calibration
            (0x0F, 'int',   'instrument_number'),            # Device ID
            (0x13, 'int',   'pressure'),                     # Water pressure
            (0x14, 'int',   'water_temperature'),            # Temperature
            (0x16, 'float', 'instantaneous_heat'),           # Heat flow
            (0x18, 'long',  'cumulative_heat_whole'),        # Heat integer
            (0x1A, 'float', 'cumulative_heat_decimal'),      # Heat decimal
            (0x1C, 'long',  'cumulative_cold_whole'),        # Cold integer
            (0x1E, 'float', 'cumulative_cold_decimal'),      # Cold decimal
            (0x22, 'int',   'pressure_record_1'),
            (0x23, 'int',   'pressure_record_2'),
            (0x24, 'int',   'pressure_record_3'),
            (0x28, 'float', 'flow_record_1'),
            (0x2A, 'float', 'flow_record_2'),
            (0x30, 'float', 'flow_record_5')
        ]

    def decode_packet(self, data_hex):
        try:
            # Protocol structure: NB[Header],[IMEI],[DATA],END
            raw_bytes = bytes.fromhex(data_hex)
            segments = raw_bytes.split(b',')
            
            if len(segments) < 3:
                return None
            
            data_field = segments[2]
            results = {}

            for addr, dtype, name in self.mapping:
                offset = addr * 2
                
                if dtype == 'float':
                    chunk = data_field[offset:offset+4]
                    if len(chunk) == 4:
                        results[name] = round(struct.unpack('>f', chunk)[0], 4)
                elif dtype == 'long':
                    chunk = data_field[offset:offset+4]
                    if len(chunk) == 4:
                        results[name] = struct.unpack('>l', chunk)[0]
                elif dtype == 'int':
                    chunk = data_field[offset:offset+2]
                    if len(chunk) == 2:
                        results[name] = struct.unpack('>h', chunk)[0]
                else:
                    results[name] = 0
            return results
        except Exception:
            return None

    def monitor_and_save(self, input_file, output_file):
        print(f"Monitoring {input_file}... Saving to {output_file}")
        
        # Open output file in append mode ('a')
        with open(input_file, 'r') as f_in, open(output_file, 'a') as f_out:
            while True:
                line = f_in.readline()
                if not line:
                    time.sleep(0.5)
                    continue
                
                try:
                    record = json.loads(line)
                    decoded_measurements = self.decode_packet(record.get("data_hex", ""))
                    
                    if decoded_measurements:
                        # Construct the requested JSON format
                        output_entry = {
                            "timestamp": record.get("timestamp"),
                            "imei": record.get("imei"),
                            "decoded_measurements": decoded_measurements
                        }
                        
                        # Write to JSONL file
                        f_out.write(json.dumps(output_entry) + "\n")
                        f_out.flush() # Ensure data is written immediately
                        
                        print(f"Processed: {record.get('timestamp')} | IMEI: {record.get('imei')}")
                except Exception as e:
                    print(f"Error: {e}")

if __name__ == "__main__":
    decoder = FlowMeterAccurateDecoder()
    # Continuous running loop
    decoder.monitor_and_save('modem_data.jsonl', 'decord_result.jsonl')