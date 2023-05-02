from credentials import question


query = """
SELECT "Wpi_country_code", COUNT(*) as port_count
FROM "WPI_data"
WHERE "Load_offload_wharves" = 'Y'
GROUP BY "Wpi_country_code"
ORDER BY port_count DESC
LIMIT 1;
        """

question(query,2)