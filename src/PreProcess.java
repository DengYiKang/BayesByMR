
import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

/**
 * 预处理
 */
public class PreProcess {

    final static String DATA_PATH = "data";
    final static String MERGE_PATH = "merge";
    final static String CLASS_A = "AUSTR";
    final static String CLASS_B = "BRAZ";
    final static String CLASS_A_PATH = DATA_PATH + File.separator + CLASS_A;
    final static String CLASS_B_PATH = DATA_PATH + File.separator + CLASS_B;
    final static String RENAME_PATH = DATA_PATH + File.separator + "RENAME";
    final static String TEST_PATH = DATA_PATH + File.separator + "TEST";
    final static String TRAIN_PATH = DATA_PATH + File.separator + "TRAIN";
    final static String CLASS_A_TRAIN_PATH = TRAIN_PATH + File.separator + CLASS_A;
    final static String CLASS_B_TRAIN_PATH = TRAIN_PATH + File.separator + CLASS_B;
    final static String CLASS_A_TEST_PATH = TEST_PATH + File.separator + CLASS_A;
    final static String CLASS_B_TEST_PATH = TEST_PATH + File.separator + CLASS_B;
    final static String MERGE_TEST_PATH = MERGE_PATH + File.separator + "TEST";
    final static String MERGE_TRAIN_PATH = MERGE_PATH + File.separator + "TRAIN";
    final static String CLASS_A_MERGE_TRAIN_PATH = MERGE_TRAIN_PATH + File.separator + CLASS_A;
    final static String CLASS_B_MERGE_TRAIN_PATH = MERGE_TRAIN_PATH + File.separator + CLASS_B;

    public static void main(String[] args) throws IOException {
        int offset = rename(CLASS_A_PATH, CLASS_A, 0);
        rename(CLASS_B_PATH, CLASS_B, offset);
        copyDir(CLASS_A_PATH, RENAME_PATH);
        copyDir(CLASS_B_PATH, RENAME_PATH);
        randomPickAndGen(CLASS_A_PATH, CLASS_A_TRAIN_PATH, CLASS_A_TEST_PATH);
        randomPickAndGen(CLASS_B_PATH, CLASS_B_TRAIN_PATH, CLASS_B_TEST_PATH);
        merge(CLASS_A_TRAIN_PATH, MERGE_TRAIN_PATH, 0, 1, 0);
        merge(CLASS_B_TRAIN_PATH, MERGE_TRAIN_PATH, 1, 1, 0);
        merge(CLASS_A_TEST_PATH, MERGE_TEST_PATH, 0, 3, 1);
        merge(CLASS_B_TEST_PATH, MERGE_TEST_PATH, 0, 3, 1);
    }

    /**
     * 随机划分，生成训练集与测试集，数据比为3:1
     *
     * @param srcPath
     * @param trainPath
     * @param testPath
     */
    public static void randomPickAndGen(String srcPath, String trainPath, String testPath) {
        List<String> files = getFiles(srcPath);
        Random rand = new Random();
        int cnt = 0, tot = files.size(), maxCnt = tot / 4;
        HashSet<Integer> vis = new HashSet<>();
        while (cnt < maxCnt) {
            int index = rand.nextInt(tot);
            if (!vis.contains(index)) {
                vis.add(index);
                cnt++;
            }
        }
        for (int i = 0; i < tot; i++) {
            String fileName = files.get(i);
            if (vis.contains(i)) {
                copyFile(srcPath + File.separator + fileName,
                        testPath + File.separator + fileName);
            } else {
                copyFile(srcPath + File.separator + fileName,
                        trainPath + File.separator + fileName);
            }
        }
    }

    /**
     * 改名为className_offset.txt，并返回末尾offset
     *
     * @param path
     * @param className
     * @param offset
     * @return
     */
    public static int rename(String path, String className, int offset) {
        List<String> files = getFiles(path);
        for (String fileName : files) {
            File file = new File(path + File.separator + fileName);
            String newName = className + "_" + (++offset) + ".txt";
            file.renameTo(new File(path + File.separator + newName));
        }
        return offset;
    }

    /**
     * @param srcPath
     * @param destPath
     * @param offset   偏移量，生成[offset+1, offset+num]的文件
     * @param num      合并后的数量
     * @param kind
     * @return
     * @throws IOException
     */
    public static int merge(String srcPath, String destPath, int offset, int num, int kind) throws IOException {
        List<String> files = getFiles(srcPath);
        for (int i = 0; i < files.size(); i++) {
            int index = offset + i % num + 1;
            String filename = files.get(i);
            //destPath + File.separator
            String destFileStr = destPath + File.separator + index + ".txt";
            FileWriter writer = new FileWriter(destFileStr, true);
            File inputFile = new File(filename);
            BufferedReader reader = new BufferedReader(new FileReader(srcPath + File.separator + inputFile));
            String line = null;
            int pos = filename.indexOf("_");
            //对于训练集，head即类别。对于测试集，head即文件名index。
            String head = kind == 0 ? filename.substring(0, pos) : filename.substring(pos + 1);
            StringBuilder sb = new StringBuilder();
            //每行第一个字段为对应文件的类别
            sb.append(head);
            //每个文件占一行
            while ((line = reader.readLine()) != null) {
                sb.append(" " + line);
            }
            writer.write(sb.toString());
            reader.close();
            writer.write("\n");
            writer.close();
        }
        return num;
    }

    /**
     * 获取某个文件夹下的所有文件名
     *
     * @param dir 文件夹路径
     * @return 文件名集合
     */
    public static List<String> getFiles(String dir) {
        List<String> files = new ArrayList<>();
        File file = new File(dir);
        File[] tempList = file.listFiles();
        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
                String fileName = tempList[i].getName();
                files.add(fileName);
            }
        }
        return files;
    }

    /**
     * 拷贝文件夹下所有内容到另一文件夹下
     *
     * @param sourcePathDir
     * @param newPathDir
     */
    public static void copyDir(String sourcePathDir, String newPathDir) {
        File start = new File(sourcePathDir);
        File end = new File(newPathDir);
        String[] filePath = start.list();//获取该文件夹下的所有文件以及目录的名字
        if (!end.exists()) {
            end.mkdir();
        }
        boolean flag = false;
        for (String temp : filePath) {
            //添加满足情况的条件
            if (new File(sourcePathDir + File.separator + temp).isFile()) {
                //为文件则进行拷贝
                flag = copyFile(sourcePathDir + File.separator + temp, newPathDir + File.separator + temp);
            }
            if (flag) {
                System.out.println("文件:" + temp + ",复制成功！");
            } else {
                System.out.println("文件:" + temp + ",复制失败！");
            }
        }
    }

    /**
     * 文件的拷贝
     *
     * @param sourcePath
     * @param newPath
     * @return
     */
    public static boolean copyFile(String sourcePath, String newPath) {
        boolean flag = false;
        File readfile = new File(sourcePath);
        File newFile = new File(newPath);
        BufferedWriter bufferedWriter = null;
        Writer writer = null;
        FileOutputStream fileOutputStream = null;
        BufferedReader bufferedReader = null;
        try {
            fileOutputStream = new FileOutputStream(newFile, true);
            writer = new OutputStreamWriter(fileOutputStream, "UTF-8");
            bufferedWriter = new BufferedWriter(writer);

            bufferedReader = new BufferedReader(new FileReader(readfile));

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line);
                bufferedWriter.newLine();
                bufferedWriter.flush();
            }
            flag = true;
        } catch (IOException e) {
            flag = false;
            e.printStackTrace();
        } finally {
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
                if (writer != null) {
                    writer.close();
                }
                if (fileOutputStream != null) {
                    fileOutputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return flag;
    }
}
