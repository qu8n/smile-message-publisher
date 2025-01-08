#!/usr/bin/env python3

# This script helps in cases where we need to remove duplicate Patient nodes from the graph.

import json
import sys
import os


def extract_patient_ids(json_file):
    """Extract unique patient IDs from the JSON file"""

    # Check if file exists
    if not os.path.exists(json_file):
        print(f"Error: File {json_file} not found")
        sys.exit(1)

    try:
        # Read JSON file
        with open(json_file) as f:
            data = json.load(f)

        # Extract unique patient IDs
        patient_ids = set()
        for sample in data.get("samples", []):
            patient_id = sample.get("cmoPatientId")
            if patient_id:
                patient_ids.add(patient_id)

        print(f"Found {len(patient_ids)} unique patient IDs:")
        print("")

        print("Query to see which of these patient ids are connected to duplicate Patient nodes:")
        print(
            "MATCH (p:Patient)<-[:IS_ALIAS]-(pa:PatientAlias) WHERE pa.namespace = 'cmoId' AND pa.value IN ["
            + f"{', '.join([f'\"{patient_id}\"' for patient_id in patient_ids])}"
            + "] WITH pa.value AS cmoPatientId, COUNT(p) AS patientCount WHERE patientCount > 1 RETURN cmoPatientId, patientCount"
        )
        print("")

        print("Query to help figure out the canonical Patient node among the dups:")
        print("MATCH (p:Patient)<-[:IS_ALIAS]-(pa:PatientAlias) where pa.value = '<cmoPatientId with dups>' return p")
        print("")

        print("Query to connect a sample of a non-canonical Patient node to the canonical Patient node:")
        print("match (p:Patient {smilePatientId: <insert>), (s:Sample {smileSampleId: <insert>}) merge (p)-[:HAS_SAMPLE]->(s)")
        print("")

        print("Query to severe the sample above from the non-canonical Patient node:")
        print("MATCH (p:Patient)<-[ia:IS_ALIAS]-(pa:PatientAlias) where p.smilePatientId = <insert> detach delete p, pa")


    except json.JSONDecodeError:
        print(f"Error: {json_file} is not a valid JSON file")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extract_patient_ids.py <json_file>")
        sys.exit(1)

    json_file = sys.argv[1]
    extract_patient_ids(json_file)
