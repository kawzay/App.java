package dao;


import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import entity.School;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/*Класс для работы с .csv форматом*/
public class SchoolCSV {

    /*Разделитель между элементами PersonalData*/
    public static List<School> schools = new ArrayList<>();

    public static List<School> convertObjectPersonalData(List<String[]> stringData){
        List<School> schoolsList = new ArrayList<>();
        for (String[] arr :
                stringData) {
           School data = new School.Builder(Integer.parseInt(arr[0]))
                   .setDistrict(Integer.parseInt(arr[1]))
                   .setName(arr[2])
                   .setCounty(arr[3])
                   .setGrades(arr[4])
                   .setStudents(Integer.parseInt(arr[5]))
                   .setTeachers(Double.parseDouble(arr[6]))
                   .setCalworks(Double.parseDouble(arr[7]))
                   .setLunch(Double.parseDouble(arr[8]))
                   .setComputers(Integer.parseInt(arr[9]))
                   .setExpenditure(Double.parseDouble(arr[10]))
                   .setIncome(Double.parseDouble(arr[11]))
                   .setEnglish(Double.parseDouble(arr[12]))
                   .setRead(Double.parseDouble(arr[13]))
                   .setMath(Double.parseDouble(arr[14]))
                           .build();
           schoolsList.add(data);
        }

        return schoolsList;
    }

    public static boolean openFile() {

        File file = Path.of("src/main/resources/schools.csv").toFile();

        List<String[]> meta = new ArrayList<>();
        try {
            CSVReader reader  = new CSVReaderBuilder(new FileReader(file)).withSkipLines(1).build();
            meta = reader.readAll();
        } catch (IOException | CsvException e) {

            return false;
        }
        SchoolCSV.schools = SchoolCSV.convertObjectPersonalData(meta);
        return true;
    }
}
