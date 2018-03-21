public class HeapSort {
    public static void main(String[] args) {
        int[] array = {1,3,5,2,4,8,6,9,7};
        heapSort(array);
        for(int i : array){
            System.out.println(i);
        }
    }

    public static void heapSort(int[] array){
        int len = array.length-1;
        for(int i = len/2 -1;i>=0;i--){//堆构造
            heapAdjust(array,i,len);
        }
        while (len >=0){
            swap(array,0,len--); //将堆顶元素与尾节点交换后，长度减一，尾元素最大
            heapAdjust(array,0,len);//再次对堆进行调整
        }

    }

    public static void heapAdjust(int[] arr,int i ,int len){
        int left,right,j;
        while((left = 2*i +1)<=len){ //判断当前父节点有无左节点（即有无孩子节点，left为左节点）
            right = left + 1; //右节点
            j = left;// j 指向左节点
            if(j < len && arr[left] < arr[right]){ //右节点大于左节点
                j++; // 指向右节点
            }
            if(arr[i] < arr[j]){ //将父节点与孩子节点交换(如果上面 if 为真，则arr[j] 为右节点，如果为假arr[j] 为左节点)
                swap(arr,i,j);
            }
            else //说明比孩子节点都大，直接跳出循环语句
                break;
            i=len; //跳出循环
        }
    }


    public static void swap(int[] arr,int i,int j){
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }
}
