package entity;

public class School {



    public static class Builder{
        private int id;
        private int district;
        private String name;
        private String county;
        private String grades;
        private int students;
        private double teachers;
        private double calworks;
        private double lunch;
        private int computers;
        private double expenditure;
        private double income;
        private double english;
        private double read;
        private double math;

        public Builder(int id){
            this.id = id;
        }

        public Builder setDistrict(int district) {
            this.district = district;

            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setCounty(String county) {
            this.county = county;
            return this;
        }

        public Builder setGrades(String grades) {
            this.grades = grades;
            return this;
        }

        public Builder setStudents(int students) {
            this.students = students;
            return this;
        }

        public Builder setTeachers(double teachers) {
            this.teachers = teachers;
            return this;
        }

        public Builder setCalworks(double calworks) {
            this.calworks = calworks;
            return this;
        }

        public Builder setLunch(double lunch) {
            this.lunch = lunch;
            return this;
        }

        public Builder setComputers(int computers) {
            this.computers = computers;
            return this;
        }

        public Builder setExpenditure(double expenditure) {
            this.expenditure = expenditure;
            return this;
        }

        public Builder setIncome(double income) {
            this.income = income;
            return this;
        }

        public Builder setEnglish(double english) {
            this.english = english;
            return this;
        }

        public Builder setRead(double read) {
            this.read = read;
            return this;
        }

        public Builder setMath(double math) {
            this.math = math;
            return this;
        }


        public School build(){
            School school = new School();
            school.id = this.id;
            school.district = this.district;
            school.name = this.name;
            school.county = this.county;
            school.grades = this.grades;
            school.students = this.students;
            school.teachers = this.teachers;
            school.calworks = this.calworks;
            school.lunch = this.lunch;
            school.computers = this.computers;
            school.expenditure = this.expenditure;
            school.income = this.income;
            school.english = this.english;
            school.read = this.read;
            school.math = this.math;

            return school;
        }
    }


    private int id;
    private int district;
    private String name;
    private String county;
    private String grades;
    private int students;
    private double teachers;
    private double calworks;
    private double lunch;
    private int computers;
    private double expenditure;
    private double income;
    private double english;
    private double read;
    private double math;


    private School(){}

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getDistrict() {
        return district;
    }

    public void setDistrict(int district) {
        this.district = district;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCounty() {
        return county;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public String getGrades() {
        return grades;
    }

    public void setGrades(String grades) {
        this.grades = grades;
    }

    public int getStudents() {
        return students;
    }

    public void setStudents(int students) {
        this.students = students;
    }

    public double getTeachers() {
        return teachers;
    }

    public void setTeachers(double teachers) {
        this.teachers = teachers;
    }

    public double getCalworks() {
        return calworks;
    }

    public void setCalworks(double calworks) {
        this.calworks = calworks;
    }

    public double getLunch() {
        return lunch;
    }

    public void setLunch(double lunch) {
        this.lunch = lunch;
    }

    public int getComputers() {
        return computers;
    }

    public void setComputers(int computers) {
        this.computers = computers;
    }

    public double getExpenditure() {
        return expenditure;
    }

    public void setExpenditure(double expenditure) {
        this.expenditure = expenditure;
    }

    public double getIncome() {
        return income;
    }

    public void setIncome(double income) {
        this.income = income;
    }

    public double getEnglish() {
        return english;
    }

    public void setEnglish(double english) {
        this.english = english;
    }

    public double getRead() {
        return read;
    }

    public void setRead(int read) {
        this.read = read;
    }

    public double getMath() {
        return math;
    }

    public void setMath(int math) {
        this.math = math;
    }


    @Override
    public String toString() {
        return "School{" +
                "id=" + id +
                ", district=" + district +
                ", name='" + name + '\'' +
                ", county='" + county + '\'' +
                ", grades='" + grades + '\'' +
                ", students=" + students +
                ", teachers=" + teachers +
                ", calworks=" + calworks +
                ", lunch=" + lunch +
                ", computers=" + computers +
                ", expenditure=" + expenditure +
                ", income=" + income +
                ", english=" + english +
                ", read=" + read +
                ", math=" + math +
                '}';
    }
}
