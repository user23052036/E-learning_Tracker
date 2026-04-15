"""
E-Learning Progress Tracker - Student Performance Analytics
Analyzes individual student performance, engagement patterns, and at-risk identification
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from spark_utils import SparkConfig, SparkDataLoader, SparkUtils

class StudentPerformanceAnalytics:
    def __init__(self, spark, datasets):
        self.spark = spark
        self.datasets = datasets
    
    def calculate_student_performance_metrics(self):
        """Calculate comprehensive performance metrics for each student"""
        print("\n" + "="*60)
        print("Student Performance Metrics")
        print("="*60 + "\n")
        
        students = self.datasets['students']
        enrollments = self.datasets['enrollments']
        progress = self.datasets['progress']
        assessments = self.datasets['assessments']
        
        # Enrollment metrics
        enrollment_metrics = enrollments.groupBy('student_id') \
            .agg(
                count('*').alias('total_enrollments'),
                sum(when(col('status') == 'Completed', 1).otherwise(0)).alias('completed_courses'),
                sum(when(col('status') == 'Active', 1).otherwise(0)).alias('active_courses'),
                sum(when(col('status') == 'Dropped', 1).otherwise(0)).alias('dropped_courses')
            ) \
            .withColumn('completion_rate',
                       round((col('completed_courses') / col('total_enrollments')) * 100, 2))
        
        # Progress metrics
        progress_metrics = progress.groupBy('student_id') \
            .agg(
                count('*').alias('total_modules_attempted'),
                sum(when(col('completion_status') == 'Completed', 1).otherwise(0)).alias('modules_completed'),
                round(avg('time_spent_hours'), 2).alias('avg_time_per_module'),
                round(sum('time_spent_hours'), 2).alias('total_study_hours'),
                round(avg('progress_percentage'), 2).alias('avg_progress_percentage')
            ) \
            .withColumn('module_completion_rate',
                       round((col('modules_completed') / col('total_modules_attempted')) * 100, 2))
        
        # Assessment metrics
        assessment_metrics = assessments.groupBy('student_id') \
            .agg(
                count('*').alias('total_assessments'),
                round(avg('score'), 2).alias('avg_score'),
                max('score').alias('max_score'),
                min('score').alias('min_score'),
                sum(when(col('passed') == True, 1).otherwise(0)).alias('assessments_passed')
            ) \
            .withColumn('pass_rate',
                       round((col('assessments_passed') / col('total_assessments')) * 100, 2))
        
        # Combine all metrics
        student_performance = students.select('student_id', 'name', 'age', 'education_level') \
            .join(enrollment_metrics, 'student_id', 'left') \
            .join(progress_metrics, 'student_id', 'left') \
            .join(assessment_metrics, 'student_id', 'left') \
            .fillna(0)
        
        print("Student Performance Metrics:")
        student_performance.show(20, truncate=False)
        
        # Summary statistics
        print("\nOverall Performance Summary:")
        print("-" * 60)
        student_performance.select(
            round(avg('completion_rate'), 2).alias('avg_course_completion'),
            round(avg('module_completion_rate'), 2).alias('avg_module_completion'),
            round(avg('avg_score'), 2).alias('avg_assessment_score'),
            round(avg('total_study_hours'), 2).alias('avg_study_hours')
        ).show()
        
        return student_performance
    
    def identify_top_performers(self, top_n=10):
        """Identify top performing students"""
        print("\n" + "="*60)
        print(f"🏆 Top {top_n} Performing Students")
        print("="*60 + "\n")
        
        performance = self.calculate_student_performance_metrics()
        
        # Create performance score
        top_students = performance \
            .withColumn('performance_score',
                       (col('completion_rate') * 0.4 + 
                        col('module_completion_rate') * 0.3 + 
                        col('avg_score') * 0.3)) \
            .orderBy(desc('performance_score')) \
            .limit(top_n)
        
        print(f"Top {top_n} Students by Performance Score:")
        top_students.select(
            'student_id',
            'name',
            'total_enrollments',
            'completed_courses',
            'completion_rate',
            'avg_score',
            'total_study_hours',
            'performance_score'
        ).show(truncate=False)
        
        return top_students
    
    def identify_at_risk_students(self):
        """Identify students at risk of dropping out"""
        print("\n" + "="*60)
        print("At-Risk Student Identification")
        print("="*60 + "\n")
        
        performance = self.calculate_student_performance_metrics()
        
        # Define at-risk criteria
        at_risk = performance.filter(
            (col('completion_rate') < 50) |
            (col('avg_score') < 60) |
            (col('dropped_courses') > 2) |
            (col('module_completion_rate') < 40)
        ).withColumn('risk_level',
            when((col('completion_rate') < 30) & (col('avg_score') < 50), 'High')
            .when((col('completion_rate') < 50) | (col('avg_score') < 60), 'Medium')
            .otherwise('Low')
        ).orderBy(asc('completion_rate'))
        
        print(f"Found {at_risk.count()} at-risk students")
        print("\nAt-Risk Students:")
        at_risk.select(
            'student_id',
            'name',
            'risk_level',
            'total_enrollments',
            'completed_courses',
            'dropped_courses',
            'completion_rate',
            'avg_score',
            'total_study_hours'
        ).show(30, truncate=False)
        
        # Risk distribution
        print("\nRisk Level Distribution:")
        at_risk.groupBy('risk_level') \
            .count() \
            .orderBy('risk_level') \
            .show()
        
        return at_risk
    
    def analyze_engagement_patterns(self):
        """Analyze student engagement patterns"""
        print("\n" + "="*60)
        print("Student Engagement Analysis")
        print("="*60 + "\n")
        
        progress = self.datasets['progress']
        students = self.datasets['students']
        
        # Add date features to progress
        progress_with_dates = progress \
            .withColumn('start_month', month(col('start_date'))) \
            .withColumn('start_day_of_week', dayofweek(col('start_date')))
        
        # Engagement by day of week
        print("Engagement by Day of Week:")
        day_engagement = progress_with_dates.groupBy('start_day_of_week') \
            .agg(
                count('*').alias('total_activities'),
                round(avg('time_spent_hours'), 2).alias('avg_time_spent')
            ) \
            .withColumn('day_name',
                when(col('start_day_of_week') == 1, 'Sunday')
                .when(col('start_day_of_week') == 2, 'Monday')
                .when(col('start_day_of_week') == 3, 'Tuesday')
                .when(col('start_day_of_week') == 4, 'Wednesday')
                .when(col('start_day_of_week') == 5, 'Thursday')
                .when(col('start_day_of_week') == 6, 'Friday')
                .otherwise('Saturday')
            ) \
            .orderBy('start_day_of_week')
        
        day_engagement.select('day_name', 'total_activities', 'avg_time_spent').show()
        
        # Engagement by month
        print("\nEngagement by Month:")
        month_engagement = progress_with_dates.groupBy('start_month') \
            .agg(
                count('*').alias('total_activities'),
                count_distinct('student_id').alias('active_students'),
                round(avg('time_spent_hours'), 2).alias('avg_time_spent')
            ) \
            .orderBy('start_month')
        
        month_engagement.show()
        
        # Student activity levels
        print("\nStudent Activity Levels:")
        activity_levels = progress.groupBy('student_id') \
            .agg(
                count('*').alias('total_activities'),
                round(sum('time_spent_hours'), 2).alias('total_hours')
            ) \
            .withColumn('activity_level',
                when(col('total_hours') > 100, 'Very Active')
                .when(col('total_hours') > 50, 'Active')
                .when(col('total_hours') > 20, 'Moderate')
                .otherwise('Low Activity')
            )
        
        activity_levels.groupBy('activity_level') \
            .count() \
            .orderBy(desc('count')) \
            .show()
        
        return {
            'day_engagement': day_engagement,
            'month_engagement': month_engagement,
            'activity_levels': activity_levels
        }
    
    def analyze_assessment_performance(self):
        """Detailed assessment performance analysis"""
        print("\n" + "="*60)
        print("Assessment Performance Analysis")
        print("="*60 + "\n")
        
        assessments = self.datasets['assessments']
        students = self.datasets['students']
        courses = self.datasets['courses']
        
        # Performance by attempt
        print("Performance by Attempt Number:")
        attempt_performance = assessments.groupBy('attempt_number') \
            .agg(
                count('*').alias('total_attempts'),
                round(avg('score'), 2).alias('avg_score'),
                round(avg('time_taken_minutes'), 2).alias('avg_time_taken'),
                round(sum(when(col('passed') == True, 1).otherwise(0)) / count('*') * 100, 2).alias('pass_rate')
            ) \
            .orderBy('attempt_number')
        
        attempt_performance.show()
        
        # Score distribution
        print("\nScore Distribution:")
        score_distribution = assessments \
            .withColumn('score_range',
                when(col('score') >= 90, '90-100 (A)')
                .when(col('score') >= 80, '80-89 (B)')
                .when(col('score') >= 70, '70-79 (C)')
                .when(col('score') >= 60, '60-69 (D)')
                .otherwise('Below 60 (F)')
            ) \
            .groupBy('score_range') \
            .count() \
            .orderBy(desc('count'))
        
        score_distribution.show()
        
        # Students with perfect scores
        print("\nPerfect Scores (100):")
        perfect_scores = assessments.filter(col('score') == 100) \
            .join(students.select('student_id', 'name'), 'student_id') \
            .join(courses.select('course_id', 'course_name'), 'course_id') \
            .select('student_id', 'name', 'course_name', 'assessment_date') \
            .orderBy('student_id')
        
        print(f"Total perfect scores: {perfect_scores.count()}")
        perfect_scores.show(20, truncate=False)
        
        return {
            'attempt_performance': attempt_performance,
            'score_distribution': score_distribution,
            'perfect_scores': perfect_scores
        }
    
    def compare_student_vs_average(self, student_id):
        """Compare individual student performance vs class average"""
        print("\n" + "="*60)
        print(f"Student {student_id} vs Class Average")
        print("="*60 + "\n")
        
        performance = self.calculate_student_performance_metrics()
        
        # Get student data
        student_data = performance.filter(col('student_id') == student_id)
        
        if student_data.count() == 0:
            print(f"Student {student_id} not found!")
            return None
        
        # Calculate averages
        class_avg = performance.select(
            round(avg('completion_rate'), 2).alias('avg_completion_rate'),
            round(avg('avg_score'), 2).alias('avg_score'),
            round(avg('total_study_hours'), 2).alias('avg_study_hours')
        ).first()
        
        print("Student Performance:")
        student_data.select(
            'student_id',
            'name',
            'completion_rate',
            'avg_score',
            'total_study_hours'
        ).show(truncate=False)
        
        print("\nClass Averages:")
        print(f"  Completion Rate: {class_avg['avg_completion_rate']}%")
        print(f"  Average Score: {class_avg['avg_score']}")
        print(f"  Study Hours: {class_avg['avg_study_hours']}")
        
        # Comparison
        student_row = student_data.first()
        print("\nComparison:")
        print(f"  Completion Rate: {'Above' if student_row['completion_rate'] > class_avg['avg_completion_rate'] else 'Below'} average")
        print(f"  Assessment Score: {'Above' if student_row['avg_score'] > class_avg['avg_score'] else 'Below'} average")
        print(f"  Study Hours: {'Above' if student_row['total_study_hours'] > class_avg['avg_study_hours'] else 'Below'} average")
        
        return student_data
    
    def generate_student_report(self):
        """Generate comprehensive student analytics report"""
        print("\n" + "="*70)
        print("COMPREHENSIVE STUDENT ANALYTICS REPORT")
        print("="*70 + "\n")
        
        results = {}
        
        # Run all analyses
        results['performance_metrics'] = self.calculate_student_performance_metrics()
        results['top_performers'] = self.identify_top_performers()
        results['at_risk_students'] = self.identify_at_risk_students()
        results['engagement_patterns'] = self.analyze_engagement_patterns()
        results['assessment_analysis'] = self.analyze_assessment_performance()
        
        print("\n" + "="*70)
        print("Student Analytics Report Generated Successfully!")
        print("="*70 + "\n")
        
        return results


def main():
    """Main execution function"""
    # Initialize Spark
    spark = SparkConfig.create_spark_session("StudentPerformanceAnalytics")
    
    try:
        # Load datasets
        loader = SparkDataLoader(spark)
        
        # Try HDFS first, fallback to local
        try:
            datasets = loader.load_all_datasets('/user/elearning/raw', source='hdfs')
        except:
            print("HDFS not available, loading from local...")
            datasets = loader.load_all_datasets('data/raw', source='local')
        
        # Create analytics instance
        analytics = StudentPerformanceAnalytics(spark, datasets)
        
        # Generate complete report
        report = analytics.generate_student_report()
        
        # Save results
        print("\nSaving results...")
        
        output_base = 'data/results/student_analytics'
        
        SparkUtils.save_to_csv(
            report['performance_metrics'],
            f'{output_base}/performance_metrics'
        )
        
        SparkUtils.save_to_csv(
            report['at_risk_students'],
            f'{output_base}/at_risk_students'
        )
        
        SparkUtils.save_to_csv(
            report['top_performers'],
            f'{output_base}/top_performers'
        )
        
        print("All results saved successfully!")
        
        # Example: Compare specific student
        # analytics.compare_student_vs_average('STU00001')
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark
        SparkConfig.stop_spark_session(spark)


if __name__ == "__main__":
    main()