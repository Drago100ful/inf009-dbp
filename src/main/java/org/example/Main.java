package org.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;

public class Main {

    private static final int AMOUNT = 5_000_000;
    private static final float WEIGHT = 0.20f;
    private static final boolean isLocal = true;

    public static void main(String[] args) {
        ArrayList<Person> people = new ArrayList<>();
        Connection con;
        try {
            con =
                    DriverManager.getConnection(
                            "jdbc:oracle:thin:user/pass@"
                                    + (isLocal ? "localhost" : "remote")
                                    + ":1521:sid");
            con.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {
            HashMap<Integer, String> fFRank = new HashMap<>();
            HashMap<Integer, String> mFRank = new HashMap<>();
            HashMap<Integer, String> lRank = new HashMap<>();
            HashMap<String, Float> cRank = new HashMap<>();
            HashMap<String, String> cName = new HashMap<>();
            ArrayList<String> street = new ArrayList<>();

            Statement statement = con.createStatement();
            statement.setFetchSize(2500);
            statement.execute("TRUNCATE TABLE PERSON2");
            statement.execute("ALTER SEQUENCE PERSON2_SEQ RESTART START WITH 1");

            PreparedStatement insert =
                    con.prepareStatement(
                            "INSERT INTO PERSON2 (PID, VORNAME, NACHNAME, GEBURTSDATUM, GO_PLZ, GEBURTSORT, WO_PLZ, WOHNORT, BEMERKUNG, STRASSE, HAUSNUMMER) VALUES (PERSON2_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            Random random = new Random();
            ResultSet rs;

            long timeStart = System.nanoTime();
            long timeInit = timeStart;

            rs = statement.executeQuery("SELECT Name, Rang FROM VORNAMEN WHERE ID < 744");

            while (rs.next()) {
                fFRank.put(rs.getInt("Rang"), rs.getString("Name"));
            }
            rs.close();

            rs = statement.executeQuery("SELECT Name, Rang FROM VORNAMEN WHERE ID >= 744");

            while (rs.next()) {
                mFRank.put(rs.getInt("Rang"), rs.getString("Name"));
            }
            rs.close();

            rs = statement.executeQuery("SELECT Name, Rang FROM NACHNAMEN");

            while (rs.next()) {
                lRank.put(
                        Integer.parseInt(rs.getString("Rang").replaceAll("[\\s.]", "")),
                        rs.getString("Name"));
            }
            rs.close();

            rs =
                    statement.executeQuery(
                            "SELECT ewPLZ AS PLZ, ORT, EW * 100 / TOTAL REL FROM (SELECT PLZ ewPLZ, EINWOHNER EW, SUM(EINWOHNER) OVER () TOTAL FROM PLZEW) JOIN PLZORT ort ON PLZ = ewPLZ");

            while (rs.next()) {
                cRank.put(rs.getString("PLZ"), rs.getFloat("REL"));
                cName.put(rs.getString("PLZ"), rs.getString("ORT"));
            }

            rs.close();

            rs = statement.executeQuery("SELECT NAME FROM STRASSE");

            while (rs.next()) {
                street.add(rs.getString("NAME"));
            }

            rs.close();
            statement.close();

            System.out.printf(
                    "Fetching data took: %fs%n", (float) ((System.nanoTime() - timeStart) * 1E-9));

            int size = fFRank.size() + mFRank.size();
            int count = AMOUNT / 2;
            int res;
            float f;
            float r;

            timeStart = System.nanoTime();
            for (Map.Entry<Integer, String> pair : fFRank.entrySet()) {
                f = pair.getKey();
                res =
                        1
                                + (int)
                                        Math.round(
                                                (WEIGHT
                                                                * Math.pow(
                                                                        (((double) (f - 1) / size)
                                                                                - 1),
                                                                        2))
                                                        * count);

                res = (int) (res * random.nextFloat(0.95f, 1.05f));

                if (count < res) {
                    res = count;
                }

                for (int i = 0; i < res; i++) {
                    Person a = new Person();
                    a.firstName = pair.getValue();
                    people.add(a);
                }

                count -= res;

                if (count <= 0) {
                    break;
                }
            }

            count = AMOUNT / 2;
            for (Map.Entry<Integer, String> pair : mFRank.entrySet()) {
                f = pair.getKey();
                res =
                        1
                                + (int)
                                        Math.round(
                                                (WEIGHT
                                                                * Math.pow(
                                                                        (((double) (f - 1) / size)
                                                                                - 1),
                                                                        2))
                                                        * count);

                res = (int) (res * random.nextFloat(0.95f, 1.05f));

                if (count < res) {
                    res = count;
                }

                for (int i = 0; i < res; i++) {
                    Person a = new Person();
                    a.firstName = pair.getValue();
                    people.add(a);
                }

                count -= res;

                if (count <= 0) {
                    break;
                }
            }

            Collections.shuffle(people);

            size = lRank.size();
            count = AMOUNT;
            int index = 0;
            for (Map.Entry<Integer, String> pair : lRank.entrySet()) {

                res =
                        1
                                + (int)
                                        Math.round(
                                                ((WEIGHT)
                                                                * Math.pow(
                                                                        (((double)
                                                                                                (pair
                                                                                                                .getKey()
                                                                                                        - 1)
                                                                                        / size)
                                                                                - 1),
                                                                        2))
                                                        * count);

                res = (int) (res * random.nextFloat(0.95f, 1.05f));

                if (count < res) {
                    res = count;
                }

                for (int i = 0; i < res; i++, index++) {
                    people.get(index).lastName = pair.getValue();
                }

                count -= res;

                if (count <= 0) {
                    break;
                }
            }

            Collections.shuffle(people);

            count = AMOUNT;
            index = 0;
            for (Map.Entry<String, Float> pair : cRank.entrySet()) {
                f = pair.getValue();
                res = 1 + (int) (count * f);

                res = (int) (res * random.nextFloat(0.95f, 1.05f));

                if (count < res) {
                    res = count;
                }

                for (int i = 0; i < res; i++, index++) {
                    r = random.nextFloat();
                    people.get(index).birthCity = cName.get(pair.getKey());
                    people.get(index).birthZip = pair.getKey();

                    if (r > 0.95) {
                        String a = String.valueOf(random.nextInt(1067, 99998));

                        while (!cName.containsKey(a)) {
                            a = String.valueOf(random.nextInt(1067, 99998));
                        }

                        people.get(index).city = cName.get(a);
                        people.get(index).zip = a;
                    } else {
                        people.get(index).city = cName.get(pair.getKey());
                        people.get(index).zip = pair.getKey();
                    }
                }

                count -= res;

                if (count <= 0) {
                    break;
                }
            }

            Collections.shuffle(people);

            int streetCount = street.size();
            for (Person person : people) {
                person.age = random.nextInt(120);
                person.streetNo = random.nextInt(0, 100);
                person.street = street.get(random.nextInt(0, streetCount));
                person.birthdate =
                        LocalDate.now()
                                .minusYears(person.age)
                                .minusDays(random.nextInt(0, 180))
                                .toString();
            }

            System.out.printf(
                    "Generating people took: %fs%n",
                    (float) ((System.nanoTime() - timeStart) * 1E-9));

            timeStart = System.nanoTime();

            File csv = new File("output.csv");

            try (PrintWriter pw =
                    new PrintWriter(
                            new OutputStreamWriter(
                                    new FileOutputStream(csv), StandardCharsets.UTF_8),
                            false)) {
                pw.println(
                        "firstname,lastname,street,birthzip,zip,birthdate,birthcity,city,streetno");
                for (Person person : people) {
                    pw.println(
                            person.firstName
                                    + ","
                                    + person.lastName
                                    + ","
                                    + person.street
                                    + ","
                                    + person.birthZip
                                    + ","
                                    + person.zip
                                    + ","
                                    + person.birthdate
                                    + ","
                                    + person.birthCity
                                    + ","
                                    + person.city
                                    + ","
                                    + person.streetNo);
                }
                pw.flush();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            System.out.printf(
                    "Generating CSV took: %fs%n", (float) ((System.nanoTime() - timeStart) * 1E-9));
            timeStart = System.nanoTime();

            ProcessBuilder processBuilder = new ProcessBuilder();

            processBuilder.command(
                    "bash",
                    "-c",
                    "cd /bin/ && export ORACLE_SID=dbprak2 && export ORAENV_ASK=NO && . oraenv && cd ~/Schreibtisch/ && sqlldr berndt/dbprakwise23 control=import.ctl log='Result.log'");

            try {

                Process process = processBuilder.start();

                StringBuilder output = new StringBuilder();

                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(process.getInputStream()));

                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line + "\n");
                }

                int exitVal = process.waitFor();
                if (exitVal == 0) {
                    System.out.println("Success!");
                    System.out.println(output);
                    System.exit(0);
                } else {
                    // abnormal...
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Batch-insert ohne SQL Loader:

            //            for (Person person : people) {
            //                insert.setString(1, person.firstName);
            //                insert.setString(2, person.lastName);
            //                insert.setDate(3, Date.valueOf(person.birthdate));
            //                insert.setString(4, person.birthZip);
            //                insert.setString(5, person.birthCity);
            //                insert.setString(6, person.zip);
            //                insert.setString(7, person.city);
            //                insert.setString(8, null);
            //                insert.setString(9, person.street);
            //                insert.setString(10, String.valueOf(person.streetNo));
            //                insert.addBatch();
            //            }
            //            insert.executeBatch();

            System.out.printf(
                    "Insert Batch took: %fs%n", (float) ((System.nanoTime() - timeStart) * 1E-9));
            System.out.printf(
                    "Inserting %d took: %fs%n",
                    AMOUNT, (float) ((System.nanoTime() - timeInit) * 1E-9));
            insert.close();
            con.commit();
            con.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

class Person {
    String firstName = null;
    String lastName = null;
    String street = null;
    String birthZip = null;
    String zip = null;
    String birthdate = null;
    String birthCity = null;
    String city = null;
    int streetNo = -1;
    int age = -1;
}
