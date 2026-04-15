"""
E-Learning Progress Tracker - Predictive Analytics
Predicts student outcomes and provides recommendations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from spark_utils import SparkConfig, SparkDataLoader, SparkUtils
import pandas as pd

class PredictiveAnalytics:
    def __init__(self, spark, datasets):
        self.spark = spark
        self.datasets = datasets
    
    def predict_course_completion(self):
        """Predict likelihood of course completion based on early progress"""
        print("\n" + "="*60)
        print("Course Completion Prediction")
        print("="*60 + "\n")
        
        enrollments = self.datasets['enrollments']
        progress = self.datasets['progress']
        modules = self.datasets['modules']
        
        # Calculate early progress indicators (first 25% of modules)
        modules_per_course = modules.groupBy('course_id').count().withColumnRenamed('count', 'total_modules')
        
        # Get progress for each student-course pair
        student_progress = progress.groupBy('student_id', 'course_id') \
            .agg(
                count('*').alias('modules_attempted'),
                sum(when(col('completion_status') == 'Completed', 1).otherwise(0)).alias('modules_completed'),
                round(avg('time_spent_hours'), 2).alias('avg_time_spent'),
                round(avg('progress_percentage'), 2).alias('avg_progress_pct')
            )
        
        # Join with total modules and enrollment status
        prediction_data = student_progress \
            .join(modules_per_course, 'course_id') \
            .join(enrollments.select('student_id', 'course_id', 'status'), ['student_id', 'course_id']) \
            .withColumn('progress_ratio', col('modules_attempted') / col('total_modules')) \
            .withColumn('completion_ratio', col('modules_completed') / col('modules_attempted'))
        
        # Simple prediction logic based on early indicators
        predictions = prediction_data.withColumn('predicted_completion',
            when(
                (col('progress_ratio') >= 0.25) & 
                (col('completion_ratio') >= 0.7) & 
                (col('avg_time_spent') > 2),
                'High'
            ).when(
                (col('progress_ratio') >= 0.15) & 
                (col('completion_ratio') >= 0.5),
                'Medium'
            ).otherwise('Low')
        )
        
        print("Completion Predictions:")
        predictions.select(
            'student_id',
            'course_id',
            'status',
            'modules_attempted',
            'modules_completed',
            'completion_ratio',
            'avg_time_spent',
            'predicted_completion'
        ).show(30, truncate=False)
        
        # Accuracy check for completed courses
        print("\nPrediction Accuracy (for completed courses):")
        accuracy = predictions.filter(col('status') == 'Completed') \
            .groupBy('predicted_completion') \
            .count() \
            .orderBy(desc('count'))
        
        accuracy.show()
        
        return predictions
    
    def identify_struggling_students_early(self):
        """Identify students who might struggle based on early performance"""
        print("\n" + "="*60)
        print("Early Struggling Student Identification")
        print("="*60 + "\n")
        
        progress = self.datasets['progress']
        assessments = self.datasets['assessments']
        
        # Early progress metrics (first 3 modules)
        early_progress = progress.withColumn(
            'row_num',
            row_number().over(Window.partitionBy('student_id', 'course_id').orderBy('start_date'))
        ).filter(col('row_num') <= 3)
        
        early_metrics = early_progress.groupBy('student_id', 'course_id') \
            .agg(
                round(avg('time_spent_hours'), 2).alias('early_avg_time'),
                round(avg('progress_percentage'), 2).alias('early_avg_progress'),
                count('*').alias('early_modules_attempted')
            )
        
        # Early assessment scores
        early_assessments = assessments.withColumn(
            'row_num',
            row_number().over(Window.partitionBy('student_id', 'course_id').orderBy('assessment_date'))
        ).filter(col('row_num') <= 3)
        
        early_scores = early_assessments.groupBy('student_id', 'course_id') \
            .agg(
                round(avg('score'), 2).alias('early_avg_score'),
                count('*').alias('early_assessments_taken')
            )
        
        # Combine metrics
        struggling_indicators = early_metrics.join(
            early_scores,
            ['student_id', 'course_id'],
            'left'
        ).fillna(0)
        
        # Flag struggling students
        struggling = struggling_indicators.withColumn('risk_flag',
            when(
                (col('early_avg_time') < 2) |
                (col('early_avg_progress') < 40) |
                (col('early_avg_score') < 60),
                True
            ).otherwise(False)
        ).filter(col('risk_flag') == True)
        
        print(f"Found {struggling.count()} students showing early signs of struggle")
        print("\nStruggling Students (Early Indicators):")
        struggling.select(
            'student_id',
            'course_id',
            'early_modules_attempted',
            'early_avg_time',
            'early_avg_progress',
            'early_avg_score'
        ).show(30, truncate=False)
        
        return struggling
    
    def recommend_courses(self, student_id):
        """Recommend courses to a student based on their history"""
        print("\n" + "="*60)
        print(f"Course Recommendations for Student {student_id}")
        print("="*60 + "\n")
        
        enrollments = self.datasets['enrollments']
        courses = self.datasets['courses']
        assessments = self.datasets['assessments']
        
        # Get student's completed courses
        student_enrollments = enrollments.filter(col('student_id') == student_id)
        
        if student_enrollments.count() == 0:
            print(f"Student {student_id} not found!")
            return None
        
        completed_courses = student_enrollments.filter(col('status') == 'Completed') \
            .select('course_id')
        
        # Get categories of completed courses
        completed_categories = completed_courses.join(
            courses.select('course_id', 'category'),
            'course_id'
        ).select('category').distinct()
        
        # Get student's average score
        student_avg_score = assessments.filter(col('student_id') == student_id) \
            .select(avg('score').alias('avg_score')) \
            .first()
        
        if student_avg_score and student_avg_score['avg_score']:
            avg_score = student_avg_score['avg_score']
            
            # Recommend based on performance and interests
            if avg_score >= 80:
                recommended_difficulty = ['Advanced', 'Intermediate']
            elif avg_score >= 60:
                recommended_difficulty = ['Intermediate', 'Beginner']
            else:
                recommended_difficulty = ['Beginner']
        else:
            recommended_difficulty = ['Beginner', 'Intermediate']
        
        # Find courses not yet enrolled
        all_courses = courses.select('course_id')
        enrolled_courses = student_enrollments.select('course_id')
        
        available_courses = all_courses.join(
            enrolled_courses,
            'course_id',
            'left_anti'
        )
        
        # Get recommendations
        recommendations = available_courses.join(courses, 'course_id') \
            .filter(col('difficulty_level').isin(recommended_difficulty)) \
            .select(
                'course_id',
                'course_name',
                'category',
                'difficulty_level',
                'rating',
                'duration_weeks'
            ) \
            .orderBy(desc('rating')) \
            .limit(10)
        
        print(f"Student's Average Score: {avg_score if student_avg_score and student_avg_score['avg_score'] else 'N/A'}")
        print(f"Recommended Difficulty Levels: {', '.join(recommended_difficulty)}\n")
        print("Top 10 Recommended Courses:")
        recommendations.show(truncate=False)
        
        return recommendations
    
    def predict_study_time_needed(self):
        """Predict study time needed for course completion"""
        print("\n" + "="*60)
        print("Study Time Prediction")
        print("="*60 + "\n")
        
        progress = self.datasets['progress']
        modules = self.datasets['modules']
        enrollments = self.datasets['enrollments']
        courses = self.datasets['courses']
        
        # Calculate average time per module
        avg_time_per_module = progress.groupBy('module_id') \
            .agg(round(avg('time_spent_hours'), 2).alias('avg_actual_time'))
        
        # Join with module expected duration
        time_analysis = modules.join(
            avg_time_per_module,
            'module_id',
            'left'
        ).fillna(0)
        
        # Calculate total time per course
        course_time_prediction = time_analysis.groupBy('course_id') \
            .agg(
                sum('duration_hours').alias('expected_total_hours'),
                sum('avg_actual_time').alias('actual_avg_total_hours'),
                count('*').alias('num_modules')
            )
        
        # Join with course details
        result = course_time_prediction.join(
            courses.select('course_id', 'course_name', 'difficulty_level'),
            'course_id'
        ).withColumn(
            'time_variance_pct',
            round(((col('actual_avg_total_hours') - col('expected_total_hours')) / 
                   col('expected_total_hours')) * 100, 2)
        ).select(
            'course_id',
            'course_name',
            'difficulty_level',
            'num_modules',
            'expected_total_hours',
            'actual_avg_total_hours',
            'time_variance_pct'
        ).orderBy('course_id')
        
        print("Study Time Predictions:")
        result.show(20, truncate=False)
        
        # Summary
        print("\nSummary:")
        result.select(
            round(avg('time_variance_pct'), 2).alias('avg_time_variance_pct')
        ).show()
        
        return result
    
    def generate_predictive_report(self):
        """Generate comprehensive predictive analytics report"""
        print("\n" + "="*70)
        print("COMPREHENSIVE PREDICTIVE ANALYTICS REPORT")
        print("="*70 + "\n")
        
        results = {}
        
        # Run all predictions
        results['completion_predictions'] = self.predict_course_completion()
        results['struggling_students'] = self.identify_struggling_students_early()
        results['study_time_predictions'] = self.predict_study_time_needed()
        
        # Example recommendation
        # results['recommendations'] = self.recommend_courses('STU00001')
        
        print("\n" + "="*70)
        print("Predictive Analytics Report Generated Successfully!")
        print("="*70 + "\n")
        
        return results


def main():
    """Main execution function"""
    # Initialize Spark
    spark = SparkConfig.create_spark_session("PredictiveAnalytics")
    
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
        analytics = PredictiveAnalytics(spark, datasets)
        
        # Generate complete report
        report = analytics.generate_predictive_report()
        
        # Save results
        print("\nSaving results...")
        
        output_base = 'data/results/predictive_analytics'
        
        SparkUtils.save_to_csv(
            report['completion_predictions'],
            f'{output_base}/completion_predictions'
        )
        
        SparkUtils.save_to_csv(
            report['struggling_students'],
            f'{output_base}/struggling_students'
        )
        
        print("All results saved successfully!")
        
        # Example: Get recommendations for a specific student
        print("\n" + "="*60)
        analytics.recommend_courses('STU00001')
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark
        SparkConfig.stop_spark_session(spark)


if __name__ == "__main__":
    main()