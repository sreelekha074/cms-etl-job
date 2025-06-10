# 🏥 CMS ETL Kubernetes Job

This project runs  daily ETL job that downloads hospital-related datasets from [data.cms.gov](https://data.cms.gov/) and saves 
them as cleaned CSVs in your local directory. The job is scheduled using Kubernetes and runs inside a Docker container.

---

## ✅ Prerequisites

1. [Docker Desktop](https://www.docker.com/products/docker-desktop) installed
2. ✅ Kubernetes enabled inside Docker Desktop:
   - Open Docker Desktop → Settings → Kubernetes → ✅ "Enable Kubernetes"
3. `kubectl` command-line tool (comes with Docker Desktop)
4.  A folder on your local machine where output files will be saved

---

##  Step 1: Create  Folder

Decide where you want the processed files to be saved. For example, on your Desktop:
1. Clone the git repository using git clone https://github.com/sreelekha074/cms-etl-job.git
  this will create the folder in your current working directory
2. Go to that folder eg: cd ~/Desktop/cms-etl-job
3. mkdir -p /Processed_csvs_output   ---- output folder inside projects folder

## Step 2: Generate and Apply the Kubernetes CronJob YAML

## Creating the corn job scheduler using cat as we need the file output in the current directory but in real world we would mount the directory on Cloud 

While inside the folder where you want the files to be saved, run the following command:

1. 
cat <<EOF > cms-etl-cronjob.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: cms-etl-job
spec:
  schedule: "0 3 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: cms-etl
            image: sreelekha074/cms-etl:latest
            imagePullPolicy: IfNotPresent
            env:
            - name: OUTPUT_DIR
              value: /output
            volumeMounts:
            - name: host-output
              mountPath: /output
          restartPolicy: OnFailure
          volumes:
          - name: host-output
            hostPath:
              path: "$(pwd)/Processed_csvs_output"
              type: DirectoryOrCreate
EOF

Step 3: Apply the CronJob

1. kubectl apply -f cms-etl-cronjob.yaml

Step 4: Test It Manually (Optional)

1. kubectl create job --from=cronjob/cms-etl-job manual-etl-run
2. kubectl logs job/manual-etl-run

Step 5: Check Output

Your output directory (e.g., ~/Desktop/cms-output) will contain:

1. CSV files (converted to snake_case)
2. A tracking file: metadata.json

Cleanup
To delete the job later:

1. kubectl delete cronjob cms-etl-job
2. kubectl delete jobs --all
