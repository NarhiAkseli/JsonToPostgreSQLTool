using System.Diagnostics.Metrics;
using Newtonsoft.Json;
using System.Globalization;
using System.Security.Cryptography.X509Certificates;
using Npgsql;
using System.Dynamic;

namespace Deserializer
{
    public class Measurement
    {
        public int id {get; set;}
        public string? name {get; set;} 
        public float temperature {get; set;} 
        public int pressure {get; set;}
        public float humidity {get; set;}
        public string? mac {get; set;}
        public int movementCounter {get; set;}
        public DateTime ts {get; set;}
    }
 
    public class Program
    {

        public static void Main()
        {

            try
            {
                string fileName = "file.json";
                string jsonString = File.ReadAllText(fileName);
                List<Measurement> measurements = JsonConvert.DeserializeObject<List<Measurement>>(jsonString)!;
                int totalRows = measurements.Count;
                int insertedRows = 0;
                int percentage = 1;

            
                foreach (Measurement measurement in measurements)
                {

                    int success = WriteToDatabase(measurement);
                    insertedRows = insertedRows + (Convert.ToBoolean(success) ? 1 : 0);
                    int currentPercentageThreshold = totalRows / 100 * percentage;
                    if(insertedRows == currentPercentageThreshold)
                    {
                        Console.WriteLine(percentage + "%");
                        percentage++;
                    }
              
                }
            
                Console.WriteLine("Executed!");
            }

            catch(Exception e )
            {
                Console.WriteLine("Exception, " + e.Message);
            }

        }

        public static int WriteToDatabase(Measurement measurement)
        {
            try
            {
                var sql = @"INSERT INTO measurement(name, temperature, humidity, pressure, mac, movementcounter, date) VALUES(@name , @temperature, @humidity, @pressure, @mac, @movementcounter, @date)";
    
                var connectionString = "Host=localhost;Port=5432;Username=postgres;Password=example;Database=postgres";

                using NpgsqlConnection connection = new NpgsqlConnection(connectionString);
                connection.Open();
                using NpgsqlCommand cmd = new NpgsqlCommand(sql, connection);

                cmd.Parameters.AddWithValue("name", measurement.name);
                cmd.Parameters.AddWithValue("temperature", measurement.temperature);
                cmd.Parameters.AddWithValue("humidity", measurement.humidity);
                cmd.Parameters.AddWithValue("pressure", measurement.pressure);
                cmd.Parameters.AddWithValue("mac", measurement.mac);
                cmd.Parameters.AddWithValue("movementcounter", measurement.movementCounter);
                cmd.Parameters.AddWithValue("date", measurement.ts);
                return cmd.ExecuteNonQuery();
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
                Console.WriteLine(measurement.ts);
                return 0;
            }

        
        }
       
    }   
}

