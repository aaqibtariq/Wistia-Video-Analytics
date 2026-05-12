

# Create IAM Role for EC2 — so it can query Athena without hardcoded keys



- IAM → Roles → Create role → AWS service → EC2
- Name it WistiaVideoAnalytics-streamlit
- Attach these managed policies -> AmazonAthenaFullAccess, AmazonS3ReadOnlyAccess
- Also add this inline policy so Athena can write query results: json — inline policy: AthenaResultsWrite

```json

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::wistia-video-analytics-at",
        "arn:aws:s3:::wistia-video-analytics-at/*"
      ]
    }
  ]
}

```
-  Save


# Launch EC2 Instance

- Go to EC2 → Instances → Launch instances
- Name	WistiaVideoAnalytics-streamlit
- OS / AMI	Amazon Linux 2023 AMI (free tier eligible, top of the list)
- Architecture	64-bit (x86)
- Instance type	t3.small (2 vCPU, 2GB RAM — enough for Streamlit)
- Key pair	Create new key pair → name: globalpartners-key → RSA → .pem format → Download and SAVE IT
- Network settings → Security group	Create security group → name: globalpartners-streamlit-sg
- Allow SSH traffic from	My IP (auto-fills your current IP)
- Allow HTTPS traffic	Tick this
- Allow HTTP traffic	Tick this
- Storage	20 GB gp3 (default is fine)
- Advanced details → IAM instance profile	Select EC2-Streamlit-GlobalPartners (the role you just created)

## Add Port 8501 to Security Group

- EC2 → Security Groups → click globalpartners-streamlit-sg → Inbound rules → Edit inbound rules → Add rule:
- SSH	TCP	22	My IP (already there)
- Custom TCP	TCP	8501	0.0.0.0/0 (lets anyone access the dashboard)
- HTTP	TCP	80	0.0.0.0/0


  # Connect via Browser (easiest option)

- EC2 → Instances → select your instance → click Connect button → EC2 Instance Connect tab → username = ec2-user → click Connect

  # Install Everything on EC2

  ```
# Update all system packages
sudo dnf update -y

# Install Python 3.11 and pip
sudo dnf install python3.11 python3.11-pip git -y

or 
python3 -m pip install streamlit pandas plotly PyAthena

if issue with above then try

sudo ln -s /usr/bin/pip3.11 /usr/bin/pip3

pip3 install ....



# Verify Python installed correctly
python3.11 --version
# Expected: Python 3.11.x

pip3.11 --version
# Expected: pip 23.x.x

# Clone your repository

git clone https://github.com/aaqibtariq/Wistia-Video-Analytics.git

# Go into it
cd Phases/streamlit

# Check what is there
ls -la

Cd to Streamlit folder

streamlit run app.py --server.port 8501 --server.address 0.0.0.0

Then open browser with provided links

  ```
