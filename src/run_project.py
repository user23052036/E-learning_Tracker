"""
E-Learning Progress Tracker - Master Runner
Orchestrates the complete project pipeline
"""

import sys
import os
from datetime import datetime

# Import all modules
from data_generation import ELearningDataGenerator
from hdfs_operations import HDFSManager, test_hdfs_connection
from spark_utils import SparkConfig, SparkDataLoader, SparkUtils
from course_analytics import CourseAnalytics
from student_performance import StudentPerformanceAnalytics
from predictive_analysis import PredictiveAnalytics
from visualization import ELearningVisualizer

class ProjectRunner:
    def __init__(self, use_hdfs=False):
        self.use_hdfs = use_hdfs
        self.spark = None
        self.datasets = None
        self.start_time = None
        
    def print_header(self, title):
        print("\n" + "="*80)
        print(f"  {title}")
        print("="*80 + "\n")
    
    def step_1_generate_data(self):
        self.print_header("STEP 1: DATA GENERATION")
        generator = ELearningDataGenerator(num_students=500, num_courses=20, num_modules_per_course=8)
        self.datasets = generator.generate_all_data(output_dir='data/raw')
        print("\nStep 1 Complete: Data Generated\n")
        return True
    
    def step_2_setup_hdfs(self):
        if not self.use_hdfs:
            print("\nSkipping HDFS setup (use_hdfs=False)\n")
            return True
        self.print_header("STEP 2: HDFS SETUP")
        if not test_hdfs_connection():
            print("\nHDFS not available. Continuing with local mode...")
            self.use_hdfs = False
            return False
        hdfs_manager = HDFSManager()
        hdfs_manager.setup_complete_environment(local_data_dir='data/raw')
        print("\nStep 2 Complete: HDFS Setup\n")
        return True
    
    def step_3_initialize_spark(self):
        self.print_header("STEP 3: SPARK INITIALIZATION")
        self.spark = SparkConfig.create_spark_session("ELearningTracker")
        print("Step 3 Complete: Spark Initialized\n")
        return True
    
    def step_4_load_data(self):
        self.print_header("STEP 4: DATA LOADING")
        loader = SparkDataLoader(self.spark)
        if self.use_hdfs:
            self.datasets = loader.load_all_datasets('/user/elearning/raw', source='hdfs')
        else:
            self.datasets = loader.load_all_datasets('data/raw', source='local')
        print("\nStep 4 Complete: Data Loaded into Spark\n")
        return True
    
    def step_5_course_analytics(self):
        self.print_header("STEP 5: COURSE ANALYTICS")
        analytics = CourseAnalytics(self.spark, self.datasets)
        report = analytics.generate_course_report()

        output_dir = 'data/results/course_analytics'
        os.makedirs(output_dir, exist_ok=True)
        print("\nSaving course analytics results...")
        SparkUtils.save_to_csv(report['completion_rates'],    f'{output_dir}/completion_rates')
        SparkUtils.save_to_csv(report['popularity'],          f'{output_dir}/popularity')
        SparkUtils.save_to_csv(report['module_completion'],   f'{output_dir}/module_completion')
        SparkUtils.save_to_csv(report['duration'],            f'{output_dir}/course_duration')
        SparkUtils.save_to_csv(report['difficulty_analysis'], f'{output_dir}/difficulty_analysis')

        print("\nStep 5 Complete: Course Analytics\n")
        return report
    
    def step_6_student_analytics(self):
        self.print_header("STEP 6: STUDENT PERFORMANCE ANALYTICS")
        analytics = StudentPerformanceAnalytics(self.spark, self.datasets)
        report = analytics.generate_student_report()

        output_dir = 'data/results/student_analytics'
        os.makedirs(output_dir, exist_ok=True)
        print("\nSaving student analytics results...")
        SparkUtils.save_to_csv(report['performance_metrics'], f'{output_dir}/performance_metrics')
        SparkUtils.save_to_csv(report['top_performers'],      f'{output_dir}/top_performers')
        SparkUtils.save_to_csv(report['at_risk_students'],    f'{output_dir}/at_risk_students')

        engagement = report['engagement_patterns']
        SparkUtils.save_to_csv(engagement['day_engagement'],   f'{output_dir}/engagement_by_day')
        SparkUtils.save_to_csv(engagement['month_engagement'], f'{output_dir}/engagement_by_month')
        SparkUtils.save_to_csv(engagement['activity_levels'],  f'{output_dir}/activity_levels')

        assessment = report['assessment_analysis']
        SparkUtils.save_to_csv(assessment['attempt_performance'], f'{output_dir}/attempt_performance')
        SparkUtils.save_to_csv(assessment['score_distribution'],  f'{output_dir}/score_distribution')

        print("\nStep 6 Complete: Student Analytics\n")
        return report
    
    def step_7_predictive_analytics(self):
        self.print_header("STEP 7: PREDICTIVE ANALYTICS")
        analytics = PredictiveAnalytics(self.spark, self.datasets)
        report = analytics.generate_predictive_report()

        output_dir = 'data/results/predictive_analytics'
        os.makedirs(output_dir, exist_ok=True)
        print("\nSaving predictive analytics results...")
        SparkUtils.save_to_csv(report['completion_predictions'], f'{output_dir}/completion_predictions')
        SparkUtils.save_to_csv(report['struggling_students'],    f'{output_dir}/struggling_students')
        SparkUtils.save_to_csv(report['study_time_predictions'], f'{output_dir}/study_time_predictions')

        print("\nStep 7 Complete: Predictive Analytics\n")
        return report
    
    def step_8_visualizations(self):
        self.print_header("STEP 8: VISUALIZATION GENERATION")
        visualizer = ELearningVisualizer(self.datasets)
        visualizer.create_dashboard()
        print("\nStep 8 Complete: Visualizations Generated\n")
        return True
    
    def step_9_cleanup(self):
        self.print_header("STEP 9: CLEANUP")
        if self.spark:
            SparkConfig.stop_spark_session(self.spark)
        print("Step 9 Complete: Cleanup Done\n")
        return True
    
    def run_complete_pipeline(self):
        self.start_time = datetime.now()
        print("\n" + "="*80)
        print("  E-LEARNING PROGRESS TRACKER - COMPLETE PIPELINE")
        print("  Start Time:", self.start_time.strftime("%Y-%m-%d %H:%M:%S"))
        print("="*80)
        
        try:
            steps = [
                ("Data Generation",      self.step_1_generate_data),
                ("HDFS Setup",           self.step_2_setup_hdfs),
                ("Spark Initialization", self.step_3_initialize_spark),
                ("Data Loading",         self.step_4_load_data),
                ("Course Analytics",     self.step_5_course_analytics),
                ("Student Analytics",    self.step_6_student_analytics),
                ("Predictive Analytics", self.step_7_predictive_analytics),
                ("Visualizations",       self.step_8_visualizations),
                ("Cleanup",              self.step_9_cleanup)
            ]
            
            results = {}
            for step_name, step_func in steps:
                try:
                    result = step_func()
                    results[step_name] = result
                except Exception as e:
                    print(f"\nError in {step_name}: {e}")
                    import traceback
                    traceback.print_exc()
                    if step_name not in ["HDFS Setup", "Cleanup"]:
                        raise
            
            end_time = datetime.now()
            duration = (end_time - self.start_time).total_seconds()
            
            print("\n" + "="*80)
            print("PIPELINE COMPLETED SUCCESSFULLY!")
            print("="*80)
            print(f"  Start Time:    {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  End Time:      {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Duration:      {duration:.2f} seconds ({duration/60:.2f} minutes)")
            print("="*80)
            print("\nResults Location:")
            print("  - Raw Data:              data/raw/")
            print("  - Course Analytics:      data/results/course_analytics/")
            print("  - Student Analytics:     data/results/student_analytics/")
            print("  - Predictive Analytics:  data/results/predictive_analytics/")
            print("  - Visualizations:        data/results/visualizations/")
            print("="*80 + "\n")
            return results
            
        except Exception as e:
            print(f"\nPipeline failed: {e}")
            import traceback
            traceback.print_exc()
            if self.spark:
                try:
                    SparkConfig.stop_spark_session(self.spark)
                except:
                    pass
            return None


def main():
    print("""
    ╔════════════════════════════════════════════════════════════════╗
    ║                                                                ║
    ║         E-LEARNING PROGRESS TRACKER                            ║
    ║         Big Data Analytics Project                             ║
    ║                                                                ║
    ╚════════════════════════════════════════════════════════════════╝
    """)
    
    use_hdfs = False
    if len(sys.argv) > 1 and sys.argv[1] == '--hdfs':
        use_hdfs = True
        print("Running with HDFS enabled\n")
    else:
        print("Running in local mode (use --hdfs flag to enable HDFS)\n")
    
    runner = ProjectRunner(use_hdfs=use_hdfs)
    results = runner.run_complete_pipeline()
    
    if results:
        print("\nProject execution completed successfully!")
    else:
        print("\nProject execution failed. Check errors above.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()