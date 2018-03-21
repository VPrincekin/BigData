package wdh.sort;

import java.util.Arrays;

public class MergeSort {
    public static void main(String[] args) {
        int[] array = {1,3,5,7,2,4};
        array = mergeSort(array,0,array.length-1);
        for(int i :array){
            System.out.println(i);
        }

    }

    public static int[] mergeSort(int[] nums, int low, int high) {
        if (nums == null || low < 0 || low > nums.length-1 || high < 0) return nums;
        int mid = (low + high) / 2;
        if (low < high) {
            // 左边
            mergeSort(nums, low, mid);
            // 右边
            mergeSort(nums, mid + 1, high);
            // 左右归并
            merge(nums, low, mid, high);
        }
        return nums;
    }
    public static void merge(int[] nums, int low, int mid, int high) {
        int[] temp = new int[high - low + 1];
        int i = low;// 左指针
        int j = mid + 1;// 右指针
        int k = 0;
        // 把较小的数先移到新数组中
        while (i <= mid && j <= high) {
            if (nums[i] < nums[j]) {
                temp[k++] = nums[i++];
            } else {
                temp[k++] = nums[j++];
            }
        }
        // 把左边剩余的数移入数组
        while (i <= mid) {
            temp[k++] = nums[i++];
        }
        // 把右边边剩余的数移入数组
        while (j <= high) {
            temp[k++] = nums[j++];
        }
        // 把新数组中的数覆盖nums数组
        for (int k2 = 0; k2 < temp.length; k2++) {
            nums[k2 + low] = temp[k2];
        }
    }




    public static int[] MemerArray(int a[],int n,int[] b,int m,int[] c){
        int i,j,k;
        i=j=k=0;
        while (i<n && j<m){
            if(a[i]<b[j]){
                c[k++] = a[i++];
            }else
                c[k++] = b[j++];
        }
        while (i<n){
            c[k++] = a[i++];
        }
        while (j<m){
            c[k++] = b[j++];
        }
        return c;
    }
}
