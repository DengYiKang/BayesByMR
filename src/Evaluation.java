import java.io.*;
import java.util.HashSet;
import java.util.List;

public class Evaluation {

    final static String TEST_PATH = "data" + File.separator + "TEST";
    final static String CLASS_A = "AUSTR";
    final static String CLASS_B = "BRAZ";
    final static String CLASS_A_TEST_PATH = TEST_PATH + File.separator + CLASS_A;
    final static String CLASS_B_TEST_PATH = TEST_PATH + File.separator + CLASS_B;


    //TP, FP, TN, FN;
    public static void main(String[] args) throws IOException {
        float[] info_a = evaluation(CLASS_A, CLASS_A_TEST_PATH);
        float[] info_b = evaluation(CLASS_B, CLASS_B_TEST_PATH);
        float macro_p = (info_a[4] + info_b[4]) / 2;
        float macro_r = (info_a[5] + info_b[5]) / 2;
        float macro_f1 = 2 * macro_p * macro_r / (macro_p + macro_r);
        float TP = info_a[0] + info_b[0];
        float FP = info_a[1] + info_b[1];
        float TN = info_a[2] + info_b[2];
        float FN = info_a[3] + info_b[3];
        float micro_p = TP / (TP + FP);
        float micro_r = TP / (TP + FN);
        float micro_f1 = 2 * micro_p * micro_r / (micro_p + micro_r);
        System.out.println("total:");
        System.out.println("macro_p:" + macro_p + "\n" +
                "macro_r:" + macro_r + "\n" +
                "macro_f1:" + macro_f1);
        System.out.println("micro_p:" + micro_p + "\n" +
                "micro_r:" + micro_r + "\n" +
                "micro_f1:" + micro_f1);
    }

    /**
     *
     * @param className 类别C
     * @param path:真实类别C的文件夹
     * @return float[] 元素分别为TP, FP, TN, FN, precision, recall, f1
     * @throws IOException
     */
    public static float[] evaluation(String className, String path) throws IOException {
        HashSet<String> vis = new HashSet<>();
        List<String> files = PreProcess.getFiles(path);
        float TP = 0, FP = 0, TN = 0, FN = 0;
        for (String file : files) {
            int begin = file.indexOf("_");
            int end = file.indexOf(".");
            String fileIndex = file.substring(begin + 1, end);
            vis.add(fileIndex);
        }
        FileReader fileReader = new FileReader(TEST_PATH + File.separator + "part-r-00000");
        BufferedReader reader = new BufferedReader(fileReader);
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] splits = line.trim().split("\t| ");
            String curClass = splits[1];
            int pos = splits[0].indexOf(".");
            String fileIndex = splits[0].substring(0, pos);
            if (curClass.equals(className)) {
                if (vis.contains(fileIndex)) {
                    TP++;
                } else {
                    FP++;
                }
            } else {
                if (vis.contains(fileIndex)) {
                    TN++;
                } else {
                    FN++;
                }
            }
        }
        float precision = TP / (TP + FP);
        float recall = TP / (TP + FN);
        float f1 = 2 * precision * recall / (precision + recall);
        System.out.println(className + ":\n" + "precision:" + precision + '\n'
                + "recall:" + recall + "\n" + "f1:" + f1);
        return new float[]{TP, FP, TN, FN, precision, recall, f1};
    }


}
