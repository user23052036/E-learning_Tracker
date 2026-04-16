"""
E-Learning Progress Tracker - Visualization Dashboard
Creates charts and visualizations for analytics results
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import SparkSession
from spark_utils import SparkConfig, SparkDataLoader
import os

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

class ELearningVisualizer:
    def __init__(self, datasets):
        self.datasets = datasets
        self.output_dir = 'data/results/visualizations'
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _save_plot(self, filename):
        """Save plot to file"""
        filepath = f'{self.output_dir}/{filename}'
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        print(f"Saved: {filepath}")
    
    def plot_course_completion_rates(self):
        """Plot course completion rates"""
        print("\nCreating Course Completion Rate Chart...")
        
        enrollments = self.datasets['enrollments'].toPandas()
        courses = self.datasets['courses'].toPandas()
        
        # Calculate completion rates
        completion_data = enrollments.groupby('course_id')['status'].value_counts().unstack(fill_value=0)
        completion_data['total'] = completion_data.sum(axis=1)
        completion_data['completion_rate'] = (completion_data.get('Completed', 0) / completion_data['total']) * 100
        
        # Merge with course names
        completion_data = completion_data.merge(
            courses[['course_id', 'course_name']], 
            left_index=True, 
            right_on='course_id'
        ).sort_values('completion_rate', ascending=False)
        
        # Plot
        plt.figure(figsize=(14, 8))
        plt.barh(range(len(completion_data)), completion_data['completion_rate'], color='steelblue')
        plt.yticks(range(len(completion_data)), completion_data['course_name'], fontsize=9)
        plt.xlabel('Completion Rate (%)', fontsize=12)
        plt.title('Course Completion Rates', fontsize=14, fontweight='bold')
        plt.xlim(0, 100)
        
        # Add value labels
        for i, v in enumerate(completion_data['completion_rate']):
            plt.text(v + 1, i, f'{v:.1f}%', va='center', fontsize=8)
        
        plt.tight_layout()
        self._save_plot('course_completion_rates.png')
        plt.close()
    
    def plot_enrollment_trends(self):
        """Plot enrollment trends over time"""
        print("\nCreating Enrollment Trends Chart...")
        
        enrollments = self.datasets['enrollments'].toPandas()
        enrollments['enrollment_date'] = pd.to_datetime(enrollments['enrollment_date'])
        enrollments['month'] = enrollments['enrollment_date'].dt.to_period('M')
        
        # Monthly enrollments
        monthly_enrollments = enrollments.groupby('month').size()
        
        # Plot
        plt.figure(figsize=(14, 6))
        monthly_enrollments.plot(kind='line', marker='o', color='steelblue', linewidth=2)
        plt.xlabel('Month', fontsize=12)
        plt.ylabel('Number of Enrollments', fontsize=12)
        plt.title('Enrollment Trends Over Time', fontsize=14, fontweight='bold')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        self._save_plot('enrollment_trends.png')
        plt.close()
    
    def plot_student_performance_distribution(self):
        """Plot distribution of student performance scores"""
        print("\nCreating Student Performance Distribution...")
        
        assessments = self.datasets['assessments'].toPandas()
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # 1. Score distribution histogram
        axes[0, 0].hist(assessments['score'], bins=20, color='skyblue', edgecolor='black')
        axes[0, 0].set_xlabel('Score', fontsize=11)
        axes[0, 0].set_ylabel('Frequency', fontsize=11)
        axes[0, 0].set_title('Score Distribution', fontsize=12, fontweight='bold')
        axes[0, 0].axvline(assessments['score'].mean(), color='red', linestyle='--', label=f'Mean: {assessments["score"].mean():.2f}')
        axes[0, 0].legend()
        
        # 2. Box plot of scores by attempt
        assessments.boxplot(column='score', by='attempt_number', ax=axes[0, 1])
        axes[0, 1].set_xlabel('Attempt Number', fontsize=11)
        axes[0, 1].set_ylabel('Score', fontsize=11)
        axes[0, 1].set_title('Scores by Attempt Number', fontsize=12, fontweight='bold')
        plt.sca(axes[0, 1])
        plt.xticks(rotation=0)
        
        # 3. Pass rate
        pass_rate = assessments['passed'].value_counts()
        axes[1, 0].pie(pass_rate, labels=['Failed', 'Passed'], autopct='%1.1f%%', 
                       colors=['#ff9999', '#66b3ff'], startangle=90)
        axes[1, 0].set_title('Pass/Fail Distribution', fontsize=12, fontweight='bold')
        
        # 4. Time taken distribution
        axes[1, 1].hist(assessments['time_taken_minutes'], bins=20, color='lightgreen', edgecolor='black')
        axes[1, 1].set_xlabel('Time Taken (minutes)', fontsize=11)
        axes[1, 1].set_ylabel('Frequency', fontsize=11)
        axes[1, 1].set_title('Assessment Time Distribution', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        self._save_plot('student_performance_distribution.png')
        plt.close()
    
    def plot_category_popularity(self):
        """Plot course category popularity"""
        print("\nCreating Category Popularity Chart...")
        
        courses = self.datasets['courses'].toPandas()
        enrollments = self.datasets['enrollments'].toPandas()
        
        # Merge and count
        category_data = enrollments.merge(courses[['course_id', 'category']], on='course_id')
        category_counts = category_data['category'].value_counts()
        
        # Plot
        plt.figure(figsize=(10, 6))
        colors = sns.color_palette("husl", len(category_counts))
        plt.pie(category_counts, labels=category_counts.index, autopct='%1.1f%%', 
                colors=colors, startangle=140)
        plt.title('Course Enrollments by Category', fontsize=14, fontweight='bold')
        plt.tight_layout()
        self._save_plot('category_popularity.png')
        plt.close()
    
    def plot_difficulty_completion_correlation(self):
        """Plot relationship between difficulty and completion"""
        print("\nCreating Difficulty vs Completion Chart...")
        
        courses = self.datasets['courses'].toPandas()
        enrollments = self.datasets['enrollments'].toPandas()
        
        # Merge data
        merged = enrollments.merge(courses[['course_id', 'difficulty_level']], on='course_id')
        
        # Calculate completion rates by difficulty
        difficulty_completion = merged.groupby('difficulty_level')['status'].apply(
            lambda x: (x == 'Completed').sum() / len(x) * 100
        ).sort_index()
        
        # Plot
        plt.figure(figsize=(10, 6))
        bars = plt.bar(difficulty_completion.index, difficulty_completion.values, 
                       color=['green', 'orange', 'red'])
        plt.xlabel('Difficulty Level', fontsize=12)
        plt.ylabel('Completion Rate (%)', fontsize=12)
        plt.title('Completion Rate by Difficulty Level', fontsize=14, fontweight='bold')
        plt.ylim(0, 100)
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}%', ha='center', va='bottom', fontsize=11)
        
        plt.tight_layout()
        self._save_plot('difficulty_completion_correlation.png')
        plt.close()
    
    def plot_engagement_heatmap(self):
        """Plot engagement heatmap by day and month"""
        print("\nCreating Engagement Heatmap...")
        
        progress = self.datasets['progress'].toPandas()
        progress['start_date'] = pd.to_datetime(progress['start_date'])
        progress['day_of_week'] = progress['start_date'].dt.day_name()
        progress['month'] = progress['start_date'].dt.month_name()
        
        # Create pivot table
        engagement_pivot = progress.pivot_table(
            values='progress_id',
            index='day_of_week',
            columns='month',
            aggfunc='count',
            fill_value=0
        )
        
        # Reorder days
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        engagement_pivot = engagement_pivot.reindex([d for d in day_order if d in engagement_pivot.index])
        
        # Plot
        plt.figure(figsize=(12, 6))
        sns.heatmap(engagement_pivot, annot=True, fmt='g', cmap='YlOrRd', cbar_kws={'label': 'Activity Count'})
        plt.xlabel('Month', fontsize=12)
        plt.ylabel('Day of Week', fontsize=12)
        plt.title('Student Engagement Heatmap', fontsize=14, fontweight='bold')
        plt.tight_layout()
        self._save_plot('engagement_heatmap.png')
        plt.close()
    
    def plot_top_performers(self, top_n=10):
        """Plot top performing students"""
        print(f"\nCreating Top {top_n} Performers Chart...")
        
        students = self.datasets['students'].toPandas()
        assessments = self.datasets['assessments'].toPandas()
        
        # Calculate average score per student
        student_scores = assessments.groupby('student_id')['score'].mean().sort_values(ascending=False).head(top_n)
        
        # Get student names
        top_students = students[students['student_id'].isin(student_scores.index)].set_index('student_id')
        student_scores = student_scores.to_frame().join(top_students[['name']])
        
        # Plot
        plt.figure(figsize=(12, 6))
        plt.barh(range(len(student_scores)), student_scores['score'], color='gold')
        plt.yticks(range(len(student_scores)), student_scores['name'], fontsize=10)
        plt.xlabel('Average Score', fontsize=12)
        plt.title(f'Top {top_n} Performing Students', fontsize=14, fontweight='bold')
        plt.xlim(0, 100)
        
        # Add value labels
        for i, v in enumerate(student_scores['score']):
            plt.text(v + 1, i, f'{v:.1f}', va='center', fontsize=9)
        
        plt.tight_layout()
        self._save_plot('top_performers.png')
        plt.close()
    
    def plot_module_completion_funnel(self, course_id='CRS001'):
        """Plot module completion funnel for a specific course"""
        print(f"\nCreating Module Completion Funnel for {course_id}...")
        
        modules = self.datasets['modules'].toPandas()
        progress = self.datasets['progress'].toPandas()
        
        # Filter for specific course
        course_modules = modules[modules['course_id'] == course_id].sort_values('module_number')
        
        # Calculate completion for each module
        completion_data = []
        for _, module in course_modules.iterrows():
            module_progress = progress[progress['module_id'] == module['module_id']]
            completed = (module_progress['completion_status'] == 'Completed').sum()
            total = len(module_progress)
            completion_data.append({
                'module': f"Module {module['module_number']}",
                'completed': completed,
                'total': total,
                'rate': (completed / total * 100) if total > 0 else 0
            })
        
        completion_df = pd.DataFrame(completion_data)
        
        # Plot
        plt.figure(figsize=(12, 6))
        x = range(len(completion_df))
        plt.plot(x, completion_df['rate'], marker='o', linewidth=2, markersize=8, color='steelblue')
        plt.fill_between(x, completion_df['rate'], alpha=0.3, color='steelblue')
        plt.xticks(x, completion_df['module'], rotation=45)
        plt.xlabel('Module', fontsize=12)
        plt.ylabel('Completion Rate (%)', fontsize=12)
        plt.title(f'Module Completion Funnel - {course_id}', fontsize=14, fontweight='bold')
        plt.ylim(0, 100)
        plt.grid(True, alpha=0.3)
        
        # Add value labels
        for i, row in completion_df.iterrows():
            plt.text(i, row['rate'] + 2, f"{row['rate']:.1f}%", ha='center', fontsize=9)
        
        plt.tight_layout()
        self._save_plot(f'module_funnel_{course_id}.png')
        plt.close()
    
    def create_dashboard(self):
        """Create comprehensive visualization dashboard"""
        print("\n" + "="*70)
        print("CREATING COMPREHENSIVE VISUALIZATION DASHBOARD")
        print("="*70 + "\n")
        
        # Generate all visualizations
        self.plot_course_completion_rates()
        self.plot_enrollment_trends()
        self.plot_student_performance_distribution()
        self.plot_category_popularity()
        self.plot_difficulty_completion_correlation()
        self.plot_engagement_heatmap()
        self.plot_top_performers()
        self.plot_module_completion_funnel()
        
        print("\n" + "="*70)
        print(f"All visualizations saved to: {self.output_dir}/")
        print("="*70 + "\n")


def main():
    """Main execution function"""
    # Initialize Spark
    spark = SparkConfig.create_spark_session("ELearningVisualizations")
    
    try:
        # Load datasets
        loader = SparkDataLoader(spark)
        
        # Try HDFS first, fallback to local
        try:
            datasets = loader.load_all_datasets('/user/elearning/raw', source='hdfs')
        except:
            print("HDFS not available, loading from local...")
            datasets = loader.load_all_datasets('data/raw', source='local')
        
        # Create visualizer
        visualizer = ELearningVisualizer(datasets)
        
        # Generate dashboard
        visualizer.create_dashboard()
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark
        SparkConfig.stop_spark_session(spark)


if __name__ == "__main__":
    main()