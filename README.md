# E-Learning Progress Tracker

A comprehensive Big Data Analytics project for tracking and analyzing student progress in online learning platforms using **Hadoop, HDFS, YARN, Apache Spark, and PySpark**.

## 📋 Project Overview

This project implements a complete e-learning analytics system that:
- Generates synthetic student and course data
- Stores data in HDFS (Hadoop Distributed File System)
- Processes data using Apache Spark with PySpark
- Analyzes course completion rates, student performance, and engagement
- Provides predictive analytics for student success
- Creates comprehensive visualizations and dashboards

## 🎯 Features

### 1. Data Management
- Synthetic data generation for students, courses, modules, enrollments, progress, and assessments
- HDFS integration for distributed storage
- Efficient data loading and partitioning with Spark

### 2. Course Analytics
- Course completion rate analysis
- Popular and unpopular course identification
- Module-level completion tracking
- Course duration analysis
- Difficulty vs completion correlation

### 3. Student Performance Analytics
- Individual student performance metrics
- Top performer identification
- At-risk student detection
- Engagement pattern analysis
- Assessment performance tracking

### 4. Predictive Analytics
- Course completion prediction
- Early struggling student identification
- Personalized course recommendations
- Study time prediction

### 5. Visualizations
- Course completion rate charts
- Enrollment trends
- Performance distribution graphs
- Category popularity pie charts
- Engagement heatmaps
- Top performer rankings

## 🛠️ Technology Stack

- **Python 3.x**
- **Apache Hadoop 3.3+** - Distributed storage (HDFS)
- **Apache Spark 3.5+** - Distributed data processing
- **PySpark** - Python API for Spark
- **Pandas** - Data manipulation
- **Matplotlib & Seaborn** - Visualizations
- **Faker** - Synthetic data generation

## 📁 Project Structure

```
elearning-tracker/
├── data_generator.py           # Generate synthetic datasets
├── hdfs_operations.py          # HDFS management functions
├── spark_utils.py              # Spark configuration and utilities
├── course_analytics.py         # Course-level analytics
├── student_performance.py      # Student performance analytics
├── predictive_analytics.py     # Predictive models
├── visualizations.py           # Chart and dashboard generation
├── run_project.py              # Master pipeline runner
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── data/
│   ├── raw/                   # Generated CSV files
│   └── results/               # Analysis results
│       ├── course_analytics/
│       ├── student_analytics/
│       ├── predictive_analytics/
│       └── visualizations/
└── notebooks/                 # Jupyter notebooks (to be created)
    ├── 01_setup.ipynb
    ├── 02_data_generation.ipynb
    ├── 03_course_analytics.ipynb
    └── ...
```

## 🚀 Installation & Setup

### Prerequisites

1. **Java 8 or higher** (required for Hadoop/Spark)
```bash
sudo apt install default-jdk -y
java -version
```

2. **Hadoop 3.3+**
```bash
# Download and install Hadoop
cd ~/Downloads
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzvf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /opt/hadoop

# Set environment variables
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/default-java' >> ~/.bashrc
source ~/.bashrc
```

3. **Apache Spark 3.5+**
```bash
# Download and install Spark
cd ~/Downloads
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tar.gz
tar -xzvf spark-3.5.0-bin-hadoop3.tar.gz
sudo mv spark-3.5.0-bin-hadoop3 /opt/spark

# Set environment variables
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

4. **Python Dependencies**
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

### Configure Hadoop (Optional for HDFS mode)

1. Format HDFS (first time only):
```bash
hdfs namenode -format
```

2. Start Hadoop services:
```bash
start-dfs.sh
start-yarn.sh

# Verify services are running
jps
```

3. Access web interfaces:
- HDFS: http://localhost:9870
- YARN: http://localhost:8088

## 📊 Usage

### Quick Start (Local Mode)

Run the complete pipeline without HDFS:
```bash
python run_project.py
```

### With HDFS

Run with HDFS enabled:
```bash
python run_project.py --hdfs
```

### Individual Modules

You can also run individual components:

**Generate Data:**
```bash
python data_generator.py
```

**Setup HDFS:**
```bash
python hdfs_operations.py
```

**Course Analytics:**
```bash
python course_analytics.py
```

**Student Performance:**
```bash
python student_performance.py
```

**Predictive Analytics:**
```bash
python predictive_analytics.py
```

**Visualizations:**
```bash
python visualizations.py
```

## 📓 Converting to Jupyter Notebooks

To convert Python files to Jupyter notebooks:

```bash
# Install nbconvert
pip install nbconvert jupyter

# Convert individual files
jupyter nbconvert --to notebook --execute data_generator.py

# Or create notebooks manually and copy code sections
```

**Suggested Notebook Structure:**

1. `01_setup_and_environment.ipynb` - Setup instructions
2. `02_data_generation.ipynb` - Data generation code
3. `03_hdfs_operations.ipynb` - HDFS commands and operations
4. `04_course_analytics.ipynb` - Course analysis
5. `05_student_performance.ipynb` - Student metrics
6. `06_predictive_analytics.ipynb` - Predictions and recommendations
7. `07_visualizations.ipynb` - Charts and dashboards

## 🎓 Project Demonstration

### What to Show in Your Presentation

1. **Problem Statement**
   - Why track e-learning progress?
   - Challenges in traditional systems

2. **Architecture**
   - Show HDFS directory structure
   - Explain Spark processing flow
   - Demonstrate distributed computing benefits

3. **Data Pipeline**
   - Data generation process
   - HDFS storage strategy
   - Spark transformations

4. **Analytics Results**
   - Course completion insights
   - Student performance trends
   - Predictive model accuracy
   - Visualizations

5. **Technical Highlights**
   - PySpark DataFrame operations
   - Optimizations (caching, partitioning)
   - Scalability demonstrations

## 📈 Sample Outputs

The project generates:

1. **CSV Reports:**
   - `course_completion_rates.csv`
   - `student_performance_metrics.csv`
   - `at_risk_students.csv`
   - `completion_predictions.csv`

2. **Visualizations:**
   - Course completion bar charts
   - Enrollment trend lines
   - Performance distribution histograms
   - Engagement heatmaps
   - Category popularity pie charts

## 🔧 Troubleshooting

### HDFS Issues

**Port already in use:**
```bash
sudo lsof -i :9870
kill -9 <PID>
```

**Permission denied:**
```bash
hdfs dfs -chmod -R 777 /user/elearning
```

### Spark Issues

**Out of memory:**
Edit `$SPARK_HOME/conf/spark-defaults.conf`:
```
spark.driver.memory    4g
spark.executor.memory  4g
```

**Connection refused:**
Check if Hadoop services are running:
```bash
jps
# Should show: NameNode, DataNode, ResourceManager, NodeManager
```

## 🎯 Learning Outcomes

By completing this project, you will:

✅ Understand Hadoop ecosystem (HDFS, YARN, MapReduce concepts)  
✅ Master Apache Spark with PySpark DataFrames  
✅ Implement distributed data processing pipelines  
✅ Perform complex analytics on large datasets  
✅ Create predictive models with Spark  
✅ Build data visualizations  
✅ Optimize Spark jobs for performance  

## 📚 References

- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Hadoop HDFS Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)

## 🤝 Contributing

This is an academic project, but suggestions are welcome:
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📄 License

This project is created for educational purposes.

## 👨‍💻 Author

Created as part of Data Engineering coursework.

---

**Note:** This project uses synthetic data for demonstration purposes. For production use, replace with real data and implement appropriate security measures.