package org.testKitGen;

import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.util.*;
import java.util.regex.*;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.FileReader;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TestDivider {
    private Arguments arg;
    private TestTarget tt;
    private List<String> testsToExecute;
    private List<String> testsToDisplay;
    private int numOfTests;
    private String parallelmk;
    private int defaultAvgTestTime;

    public TestDivider(Arguments arg, TestTarget tt) {
        this.arg = arg;
        this.tt = tt;
        testsToExecute = TestInfo.getTestsToExecute();
        testsToDisplay = TestInfo.getTestsToDisplay();
        numOfTests = TestInfo.numOfTests();
        parallelmk = arg.getProjectRootDir() + "/TKG/" + Constants.PARALLELMK;
        defaultAvgTestTime = 40000; // in milliseconds
    }

    private void divideOnTestTime(List<List<String>> parallelLists, List<Integer> testListTime, int testTime, Queue<Map.Entry<String, Integer>> durationQueue) {
        Queue<Map.Entry<Integer, Integer>> machineQueue = new PriorityQueue<Map.Entry<Integer, Integer>>(
            new Comparator<Map.Entry<Integer, Integer>>() {
                public int compare(Map.Entry<Integer, Integer> a, Map.Entry<Integer, Integer> b) {
                    return a.getValue().equals(b.getValue()) ? a.getKey().compareTo(b.getKey()) : a.getValue().compareTo(b.getValue());
                }
            }
        );
        
        int limitFactor = testTime;
        int index = 0;
        while (!durationQueue.isEmpty()) {
            Map.Entry<String, Integer> testEntry = durationQueue.poll();
            String testName = testEntry.getKey();
            int testDuration = testEntry.getValue();
            if (!machineQueue.isEmpty() && (machineQueue.peek().getValue() + testDuration < limitFactor)) {
                Map.Entry<Integer, Integer> machineEntry = machineQueue.poll();
                parallelLists.get(machineEntry.getKey()).add(testName);
                int newTime = machineEntry.getValue() + testDuration;
                testListTime.set(machineEntry.getKey(), newTime);
                machineEntry.setValue(newTime);
                machineQueue.offer(machineEntry);
            } else {
                parallelLists.add(new ArrayList<String>());
                parallelLists.get(index).add(testName);
                testListTime.add(testDuration);
                if (testDuration < limitFactor) {
                    Map.Entry<Integer, Integer> entry = new AbstractMap.SimpleEntry<Integer, Integer>(index, testDuration);
                    machineQueue.offer(entry);
                } else {
                    limitFactor = testDuration;
                    System.out.println("Warning: Test " + testName + " has duration " + formatTime(testDuration) + ", which is greater than the specified test list execution time " + testTime + "m. So this value is used to limit the overall execution time.");
                }
                index++;
            }
        }
    }

    private void divideOnMachineNum(List<List<String>> parallelLists, List<Integer> testListTime, int numOfMachines, Queue<Map.Entry<String, Integer>> durationQueue) {
        Queue<Map.Entry<Integer, Integer>> machineQueue = new PriorityQueue<Map.Entry<Integer, Integer>>(
            new Comparator<Map.Entry<Integer, Integer>>() {
                public int compare(Map.Entry<Integer, Integer> a, Map.Entry<Integer, Integer> b) {
                    return a.getValue().equals(b.getValue()) ? a.getKey().compareTo(b.getKey()) : a.getValue().compareTo(b.getValue());
                }
            }
        );

        for (int i = 0; i < numOfMachines; i++) {
            parallelLists.add(new ArrayList<String>());
            testListTime.add(0);
            Map.Entry<Integer, Integer> entry = new AbstractMap.SimpleEntry<Integer, Integer>(i, 0);
            machineQueue.offer(entry);
        }
        while (!durationQueue.isEmpty()) {
            Map.Entry<String, Integer> testEntry = durationQueue.poll();
            Map.Entry<Integer, Integer> machineEntry = machineQueue.poll();

            parallelLists.get(machineEntry.getKey()).add(testEntry.getKey());
            int newTime = machineEntry.getValue() + testEntry.getValue();
            testListTime.set(machineEntry.getKey(), newTime);
            machineEntry.setValue(newTime);
            machineQueue.offer(machineEntry);
        }
    }

    private String constructURL(String impl, String plat, String group, String level) {
        int limit = 10;
        String URL = (arg.getTRSSURL().isEmpty() ? Constants.TRSS_URL : arg.getTRSSURL()) + "/api/getTestAvgDuration?limit=" + limit + "&jdkVersion=" + arg.getJdkVersion() + "&impl=" + impl + "&platform=" + plat;

        if (tt.isSingleTest()) {
            URL += "&testName=" + tt.getTestTargetName();
        } else if (tt.isCategory()) {
            if (!group.equals("")) {
                URL += "&group=" + group;
            }
            if (!level.equals("")) {
                URL += "&level=" + level;
            }
        }
        return URL;
    }

    private String getGroup() {
        String group = "";
        if (tt.isCategory()) {
            for (String g : Constants.ALLGROUPS) {
                if (tt.getTestCategory().getCategorySet().contains(g)) {
                    if (group.equals("")) {
                        group = g;
                    } else {
                        group = "";
                        break;
                    }
                }
            }
        }
        return group;
    }

    private String getLevel() {
        String level = "";
        if (tt.isCategory()) {
            for (String l : Constants.ALLLEVELS) {
                if (tt.getTestCategory().getCategorySet().contains(l)) {
                    if (level.equals("")) {
                        level = l;
                    } else {
                        level = "";
                        break;
                    }
                }
            }
        }
        return level;
    }

    private void parseDuration(Reader reader, Map<String, Integer> map) throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(reader);
        JSONArray testLists = (JSONArray) jsonObject.get("testLists");
        if (testLists != null && testLists.size() != 0) {
            JSONArray testList = (JSONArray) testLists.get(0);
            if (testList != null) {
                for (int i = 0; i < testList.size(); i++) {
                    JSONObject testData = (JSONObject) testList.get(i);
                    String testName = (String) testData.get("_id");
                    Number testDurationNum = (Number) testData.get("avgDuration");
                    if (testName != null && testDurationNum != null) {
                        Double testDuration = testDurationNum.doubleValue();
                        map.put(testName, testDuration.intValue());
                    }
                }
            }
        }
    }

    private Map<String, Integer> getDataFromFile() {
        String impl = arg.getBuildImpl();
        String plat = arg.getPlat();
        if (impl.equals("") || plat.equals("")) {
            return null;
        }
        String group = getGroup();
        Map<String, Integer> map = new HashMap<String, Integer>();
        System.out.println("Attempting to get test duration data from cached files.");
        String fileName = "";
        if (group.equals("")) {
            fileName = "Test_openjdk" + arg.getJdkVersion() + "_" + impl + "_(" + String.join("|", Constants.ALLGROUPS) + ")_" + plat + ".json";
        } else {
            fileName = "Test_openjdk" + arg.getJdkVersion() + "_" + impl + "_" + group + "_" + plat + ".json";
        }

        File directory = new File(arg.getProjectRootDir() + "/TKG/" + Constants.TRSSCACHE_DIR);
        Pattern p = Pattern.compile(fileName);
        File[] files = directory.listFiles(new FileFilter() {
            public boolean accept(File f) {
                return p.matcher(f.getName()).matches();
            }
        });
        if (files != null) {
            for (File f : files) {
                System.out.println("Reading file: " + f.getName());
                Reader reader = null;
                try {
                    reader = new FileReader(f);
                    parseDuration(reader, map);
                } catch (IOException | ParseException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (reader != null) reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            System.out.println("No cache files found for " + fileName);
        }
        return map;
    }

    private void processTests(List<String> tests) {
        Queue<Map.Entry<String, Integer>> durationQueue = new LinkedList<Map.Entry<String, Integer>>();
        Map<String, Integer> testDurations = getDataFromFile();

        if (testDurations == null) {
            System.out.println("Error: Could not get test durations.");
            return;
        }

        for (String test : tests) {
            Integer duration = testDurations.get(test);
            if (duration != null) {
                durationQueue.offer(new AbstractMap.SimpleEntry<String, Integer>(test, duration));
            } else {
                System.out.println("No duration found for test " + test);
            }
        }

        List<List<String>> parallelLists = new ArrayList<List<String>>();
        List<Integer> testListTime = new ArrayList<Integer>();

        int numOfMachines = arg.getNumOfMachines();
        int testTime = defaultAvgTestTime;

        if (arg.isDivideByMachineCount()) {
            divideOnMachineNum(parallelLists, testListTime, numOfMachines, durationQueue);
        } else {
            divideOnTestTime(parallelLists, testListTime, testTime, durationQueue);
        }

        // Output results
        for (int i = 0; i < parallelLists.size(); i++) {
            System.out.println("Machine " + i + ": " + parallelLists.get(i));
        }
    }

    private String formatTime(int time) {
        int minutes = time / 60;
        int seconds = time % 60;
        return String.format("%d minutes %d seconds", minutes, seconds);
    }

    public static void main(String[] args) {
        Arguments arguments = new Arguments(); // Initialize your arguments
        TestTarget target = new TestTarget();   // Initialize your target
        TestDivider divider = new TestDivider(arguments, target);
        divider.processTests(divider.testsToExecute);
    }
}
