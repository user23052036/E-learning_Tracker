"""
E-Learning Progress Tracker - Course Analytics
Analyzes course-level metrics: completion rates, popularity, drop-off points
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from spark_utils import SparkConfig, SparkDataLoader, SparkUtils

class CourseAnalytics:
    def __init__(self, spark, datasets):
        self.spark = spark
        self.datasets = datasets
        
    def calculate_course_completion_rates(self):
        """Calculate completion rate for each course"""
        print("\n" + "="*60)
        print(" Course Completion Rate Analysis")
        print("="*60 + "\n")
        
        enrollments = self.datasets['enrollments']
        courses = self.datasets['courses']
        
        # Calculate completion statistics
        completion_stats = enrollments.groupBy('course_id') \
            .agg(
                count('*').alias('total_enrollments'),
                sum(when(col('status') == 'Completed', 1).otherwise(0)).alias('completed'),
                sum(when(col('status') == 'Active', 1).otherwise(0)).alias('active'),
                sum(when(col('status') == 'Dropped', 1).otherwise(0)).alias('dropped')
            ) \
            .withColumn('completion_rate', 
                       round((col('completed') / col('total_enrollments')) * 100, 2)) \
            .withColumn('drop_rate',
                       round((col('dropped') / col('total_enrollments')) * 100, 2))
        
        # Join with course details
        result = completion_stats.join(
            courses.select('course_id', 'course_name', 'difficulty_level', 'category'),
            'course_id'
        ).select(
            'course_id',
            'course_name',
            'difficulty_level',
            'category',
            'total_enrollments',
            'completed',
            'active',
            'dropped',
            'completion_rate',
            'drop_rate'
        ).orderBy(desc('completion_rate'))
        
        print("Course Completion Statistics:")
        result.show(20, truncate=False)
        
        # Summary statistics
        print("\n Summary Statistics:")
        print("-" * 60)
        result.select(
            round(avg('completion_rate'), 2).alias('avg_completion_rate'),
            round(avg('drop_rate'), 2).alias('avg_drop_rate'),
            max('completion_rate').alias('max_completion_rate'),
            min('completion_rate').alias('min_completion_rate')
        ).show()
        
        return result
    
    def identify_popular_courses(self):
        """Identify most and least popular courses by enrollment"""
        print("\n" + "="*60)
        print(" Course Popularity Analysis")
        print("="*60 + "\n")
        
        enrollments = self.datasets['enrollments']
        courses = self.datasets['courses']
        
        popularity = enrollments.groupBy('course_id') \
            .agg(count('*').alias('enrollment_count')) \
            .join(courses.select('course_id', 'course_name', 'category', 'rating', 'price'), 
                  'course_id')
        
        print(" Top 10 Most Popular Courses:")
        popularity.orderBy(desc('enrollment_count')).limit(10).show(truncate=False)
        
        print("\n Top 10 Least Popular Courses:")
        popularity.orderBy(asc('enrollment_count')).limit(10).show(truncate=False)
        
        # Popularity by category
        print("\n Popularity by Category:")
        popularity.groupBy('category') \
            .agg(
                sum('enrollment_count').alias('total_enrollments'),
                round(avg('enrollment_count'), 2).alias('avg_enrollments_per_course')
            ) \
            .orderBy(desc('total_enrollments')) \
            .show(truncate=False)
        
        return popularity
    
    def analyze_module_completion(self):
        """Analyze completion rates at module level"""
        print("\n" + "="*60)
        print(" Module-Level Completion Analysis")
        print("="*60 + "\n")
        
        progress = self.datasets['progress']
        modules = self.datasets['modules']
        
        module_stats = progress.groupBy('module_id', 'course_id') \
            .agg(
                count('*').alias('total_attempts'),
                sum(when(col('completion_status') == 'Completed', 1).otherwise(0)).alias('completed'),
                round(avg('time_spent_hours'), 2).alias('avg_time_spent'),
                round(avg('progress_percentage'), 2).alias('avg_progress')
            ) \
            .withColumn('completion_rate',
                       round((col('completed') / col('total_attempts')) * 100, 2))
        
        # Join with module details
        result = module_stats.join(
            modules.select('module_id', 'module_name', 'module_number', 'duration_hours', 'difficulty'),
            'module_id'
        ).select(
            'course_id',
            'module_id',
            'module_name',
            'module_number',
            'difficulty',
            'duration_hours',
            'total_attempts',
            'completed',
            'completion_rate',
            'avg_time_spent',
            'avg_progress'
        ).orderBy('course_id', 'module_number')
        
        print("Module Completion Statistics:")
        result.show(30, truncate=False)
        
        # Identify bottleneck modules (lowest completion rates)
        print("\n  Bottleneck Modules (Lowest Completion Rates):")
        result.orderBy(asc('completion_rate')).limit(10).show(truncate=False)
        
        return result
    
    def analyze_course_duration(self):
        """Analyze average time to complete courses"""
        print("\n" + "="*60)
        print(" Course Duration Analysis")
        print("="*60 + "\n")
        
        enrollments = self.datasets['enrollments']
        progress = self.datasets['progress']
        courses = self.datasets['courses']
        
        # Calculate days from enrollment to completion
        completed_enrollments = enrollments.filter(col('status') == 'Completed')
        
        # Get completion dates from progress
        course_completion = progress.groupBy('student_id', 'course_id') \
            .agg(max('completion_date').alias('last_completion_date'))
        
        duration_analysis = completed_enrollments.join(
            course_completion,
            ['student_id', 'course_id']
        ).withColumn(
            'days_to_complete',
            datediff(col('last_completion_date'), col('enrollment_date'))
        )
        
        # Aggregate by course
        course_duration_stats = duration_analysis.groupBy('course_id') \
            .agg(
                count('*').alias('completions'),
                round(avg('days_to_complete'), 2).alias('avg_days_to_complete'),
                min('days_to_complete').alias('min_days'),
                max('days_to_complete').alias('max_days')
            )
        
        # Join with course details
        result = course_duration_stats.join(
            courses.select('course_id', 'course_name', 'duration_weeks', 'difficulty_level'),
            'course_id'
        ).withColumn(
            'expected_days',
            col('duration_weeks') * 7
        ).select(
            'course_id',
            'course_name',
            'difficulty_level',
            'expected_days',
            'avg_days_to_complete',
            'min_days',
            'max_days',
            'completions'
        ).orderBy('course_id')
        
        print("Course Duration Statistics:")
        result.show(20, truncate=False)
        
        # Courses faster/slower than expected
        print("\n⚡ Courses Completed Faster Than Expected:")
        result.filter(col('avg_days_to_complete') < col('expected_days')) \
            .orderBy(asc('avg_days_to_complete')) \
            .limit(5) \
            .show(truncate=False)
        
        print("\n Courses Taking Longer Than Expected:")
        result.filter(col('avg_days_to_complete') > col('expected_days')) \
            .orderBy(desc('avg_days_to_complete')) \
            .limit(5) \
            .show(truncate=False)
        
        return result
    
    def analyze_difficulty_vs_completion(self):
        """Analyze relationship between difficulty and completion rate"""
        print("\n" + "="*60)
        print(" Difficulty vs Completion Rate Analysis")
        print("="*60 + "\n")
        
        enrollments = self.datasets['enrollments']
        courses = self.datasets['courses']
        
        # Calculate completion by difficulty
        completion_by_difficulty = enrollments.join(
            courses.select('course_id', 'difficulty_level'),
            'course_id'
        ).groupBy('difficulty_level') \
         .agg(
             count('*').alias('total_enrollments'),
             sum(when(col('status') == 'Completed', 1).otherwise(0)).alias('completed'),
             sum(when(col('status') == 'Dropped', 1).otherwise(0)).alias('dropped')
         ) \
         .withColumn('completion_rate',
                    round((col('completed') / col('total_enrollments')) * 100, 2)) \
         .withColumn('drop_rate',
                    round((col('dropped') / col('total_enrollments')) * 100, 2)) \
         .orderBy('difficulty_level')
        
        print("Completion Rates by Difficulty Level:")
        completion_by_difficulty.show(truncate=False)
        
        return completion_by_difficulty
    
    def generate_course_report(self):
        """Generate comprehensive course analytics report"""
        print("\n" + "="*70)
        print(" COMPREHENSIVE COURSE ANALYTICS REPORT")
        print("="*70 + "\n")
        
        results = {}
        
        # Run all analyses
        results['completion_rates'] = self.calculate_course_completion_rates()
        results['popularity'] = self.identify_popular_courses()
        results['module_completion'] = self.analyze_module_completion()
        results['duration'] = self.analyze_course_duration()
        results['difficulty_analysis'] = self.analyze_difficulty_vs_completion()
        
        print("\n" + "="*70)
        print("Course Analytics Report Generated Successfully!")
        print("="*70 + "\n")
        
        return results


def main():
    """Main execution function"""
    # Initialize Spark
    spark = SparkConfig.create_spark_session("CourseAnalytics")
    
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
        analytics = CourseAnalytics(spark, datasets)
        
        # Generate complete report
        report = analytics.generate_course_report()
        
        # Save results
        print("\nSaving results...")
        
        output_base = 'data/results/course_analytics'
        
        SparkUtils.save_to_csv(
            report['completion_rates'],
            f'{output_base}/completion_rates'
        )
        
        SparkUtils.save_to_csv(
            report['popularity'],
            f'{output_base}/popularity'
        )
        
        SparkUtils.save_to_csv(
            report['module_completion'],
            f'{output_base}/module_completion'
        )
        
        print("All results saved successfully!")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark
        SparkConfig.stop_spark_session(spark)


if __name__ == "__main__":
    main()