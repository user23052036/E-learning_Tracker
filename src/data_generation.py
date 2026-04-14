"""
E-Learning Progress Tracker - Data Generator
Generates synthetic datasets for students, courses, modules, progress, and assessments
"""

import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import os

fake = Faker()
random.seed(42)
Faker.seed(42)

class ELearningDataGenerator:
    def __init__(self, num_students=500, num_courses=20, num_modules_per_course=8):
        self.num_students = num_students
        self.num_courses = num_courses
        self.num_modules_per_course = num_modules_per_course
        self.students = []
        self.courses = []
        self.modules = []
        self.enrollments = []
        self.progress = []
        self.assessments = []
        
    def generate_students(self):
        """Generate student data"""
        print("Generating students data...")
        
        for i in range(1, self.num_students + 1):
            student = {
                'student_id': f'STU{i:05d}',
                'name': fake.name(),
                'email': fake.email(),
                'age': random.randint(18, 45),
                'gender': random.choice(['Male', 'Female', 'Other']),
                'country': fake.country(),
                'enrollment_date': fake.date_between(start_date='-2y', end_date='today'),
                'education_level': random.choice(['High School', 'Bachelor', 'Master', 'PhD']),
                'employment_status': random.choice(['Student', 'Employed', 'Unemployed', 'Self-Employed'])
            }
            self.students.append(student)
        
        print(f"✅ Generated {len(self.students)} students")
        return pd.DataFrame(self.students)
    
    def generate_courses(self):
        """Generate course data"""
        print("Generating courses data...")
        
        course_topics = [
            'Python Programming', 'Data Science', 'Machine Learning', 'Web Development',
            'Mobile App Development', 'Cloud Computing', 'Cybersecurity', 'DevOps',
            'Artificial Intelligence', 'Blockchain', 'Digital Marketing', 'UI/UX Design',
            'Database Management', 'Software Testing', 'Project Management', 'Business Analytics',
            'IoT', 'Game Development', 'Data Engineering', 'Computer Vision'
        ]
        
        difficulty_levels = ['Beginner', 'Intermediate', 'Advanced']
        
        for i in range(1, self.num_courses + 1):
            course = {
                'course_id': f'CRS{i:03d}',
                'course_name': course_topics[i-1] if i <= len(course_topics) else f'Course {i}',
                'instructor': fake.name(),
                'difficulty_level': random.choice(difficulty_levels),
                'duration_weeks': random.randint(4, 16),
                'price': random.randint(0, 500),
                'category': random.choice(['Technology', 'Business', 'Design', 'Data Science']),
                'rating': round(random.uniform(3.5, 5.0), 1),
                'created_date': fake.date_between(start_date='-3y', end_date='-1y')
            }
            self.courses.append(course)
        
        print(f"✅ Generated {len(self.courses)} courses")
        return pd.DataFrame(self.courses)
    
    def generate_modules(self):
        """Generate module data for each course"""
        print("Generating modules data...")
        
        module_id = 1
        for course in self.courses:
            for j in range(1, self.num_modules_per_course + 1):
                module = {
                    'module_id': f'MOD{module_id:05d}',
                    'course_id': course['course_id'],
                    'module_number': j,
                    'module_name': f'{course["course_name"]} - Module {j}',
                    'duration_hours': random.randint(2, 10),
                    'content_type': random.choice(['Video', 'Reading', 'Quiz', 'Project', 'Live Session']),
                    'difficulty': random.choice(['Easy', 'Medium', 'Hard'])
                }
                self.modules.append(module)
                module_id += 1
        
        print(f"✅ Generated {len(self.modules)} modules")
        return pd.DataFrame(self.modules)
    
    def generate_enrollments(self):
        """Generate enrollment data"""
        print("Generating enrollments data...")
        
        for student in self.students:
            num_enrollments = random.randint(1, 5)
            enrolled_courses = random.sample(self.courses, num_enrollments)
            
            for course in enrolled_courses:
                enrollment = {
                    'enrollment_id': f'ENR{len(self.enrollments) + 1:06d}',
                    'student_id': student['student_id'],
                    'course_id': course['course_id'],
                    'enrollment_date': max(student['enrollment_date'], 
                                          fake.date_between(start_date='-1y', end_date='today')),
                    'status': random.choices(['Active', 'Completed', 'Dropped'], weights=[0.4, 0.4, 0.2])[0]
                }
                self.enrollments.append(enrollment)
        
        print(f"✅ Generated {len(self.enrollments)} enrollments")
        return pd.DataFrame(self.enrollments)
    
    def generate_progress(self):
        """Generate progress tracking data"""
        print("Generating progress data...")
        
        modules_df = pd.DataFrame(self.modules)
        
        for enrollment in self.enrollments:
            course_modules = modules_df[modules_df['course_id'] == enrollment['course_id']]
            
            # Determine completion pattern based on enrollment status
            if enrollment['status'] == 'Completed':
                completion_prob = 1.0
            elif enrollment['status'] == 'Dropped':
                completion_prob = random.uniform(0.1, 0.5)
            else:  # Active
                completion_prob = random.uniform(0.3, 0.9)
            
            for _, module in course_modules.iterrows():
                if random.random() < completion_prob:
                    progress = {
                        'progress_id': f'PRG{len(self.progress) + 1:07d}',
                        'student_id': enrollment['student_id'],
                        'module_id': module['module_id'],
                        'course_id': enrollment['course_id'],
                        'start_date': enrollment['enrollment_date'] + timedelta(days=random.randint(0, 30)),
                        'completion_date': enrollment['enrollment_date'] + timedelta(days=random.randint(1, 90)),
                        'completion_status': random.choices(['Completed', 'In Progress', 'Not Started'], 
                                                          weights=[0.6, 0.3, 0.1])[0],
                        'time_spent_hours': round(random.uniform(1, module['duration_hours'] * 1.5), 2),
                        'progress_percentage': random.randint(0, 100)
                    }
                    self.progress.append(progress)
        
        print(f"✅ Generated {len(self.progress)} progress records")
        return pd.DataFrame(self.progress)
    
    def generate_assessments(self):
        """Generate assessment/quiz data"""
        print("Generating assessments data...")
        
        for progress_record in self.progress:
            if progress_record['completion_status'] == 'Completed':
                # Generate assessment for completed modules
                num_attempts = random.randint(1, 3)
                
                for attempt in range(1, num_attempts + 1):
                    assessment = {
                        'assessment_id': f'ASS{len(self.assessments) + 1:07d}',
                        'student_id': progress_record['student_id'],
                        'module_id': progress_record['module_id'],
                        'course_id': progress_record['course_id'],
                        'attempt_number': attempt,
                        'score': random.randint(50, 100) if attempt == num_attempts else random.randint(30, 90),
                        'max_score': 100,
                        'assessment_date': progress_record['completion_date'] - timedelta(days=random.randint(0, 5)),
                        'time_taken_minutes': random.randint(10, 60),
                        'passed': True if random.randint(50, 100) >= 60 else False
                    }
                    self.assessments.append(assessment)
        
        print(f"✅ Generated {len(self.assessments)} assessments")
        return pd.DataFrame(self.assessments)
    
    def generate_all_data(self, output_dir='data/raw'):
        """Generate all datasets and save to CSV"""
        print("\n" + "="*60)
        print("E-Learning Data Generation Started")
        print("="*60 + "\n")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate all datasets
        students_df = self.generate_students()
        courses_df = self.generate_courses()
        modules_df = self.generate_modules()
        enrollments_df = self.generate_enrollments()
        progress_df = self.generate_progress()
        assessments_df = self.generate_assessments()
        
        # Save to CSV
        print(f"\nSaving datasets to {output_dir}...")
        students_df.to_csv(f'{output_dir}/students.csv', index=False)
        courses_df.to_csv(f'{output_dir}/courses.csv', index=False)
        modules_df.to_csv(f'{output_dir}/modules.csv', index=False)
        enrollments_df.to_csv(f'{output_dir}/enrollments.csv', index=False)
        progress_df.to_csv(f'{output_dir}/progress.csv', index=False)
        assessments_df.to_csv(f'{output_dir}/assessments.csv', index=False)
        
        print("\n" + "="*60)
        print("✅ All datasets generated successfully!")
        print("="*60)
        print(f"\nDataset Summary:")
        print(f"  - Students: {len(students_df)} records")
        print(f"  - Courses: {len(courses_df)} records")
        print(f"  - Modules: {len(modules_df)} records")
        print(f"  - Enrollments: {len(enrollments_df)} records")
        print(f"  - Progress: {len(progress_df)} records")
        print(f"  - Assessments: {len(assessments_df)} records")
        print(f"\nFiles saved in: {output_dir}/")
        print("="*60 + "\n")
        
        return {
            'students': students_df,
            'courses': courses_df,
            'modules': modules_df,
            'enrollments': enrollments_df,
            'progress': progress_df,
            'assessments': assessments_df
        }


if __name__ == "__main__":
    # Generate data
    generator = ELearningDataGenerator(
        num_students=500,
        num_courses=20,
        num_modules_per_course=8
    )
    
    datasets = generator.generate_all_data()
    
    # Display sample data
    print("\n📊 Sample Data Preview:")
    print("\n--- Students (First 5) ---")
    print(datasets['students'].head())
    
    print("\n--- Courses (First 5) ---")
    print(datasets['courses'].head())
    
    print("\n--- Progress (First 5) ---")
    print(datasets['progress'].head())