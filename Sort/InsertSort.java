package wdh.sort;
//插入排序
public class InsertSort {
	public static void main(String[] args) {
		Integer[] array={5,4,6,1,2,3,9,8,7};
		Integer[] array1 = sort(array);
		for(int i:array1){
			System.out.print(i);
		}
	}
	public static Integer[] sort(Integer[] array){
		int n=array.length;
		for(int i=1;i<n;i++){
			for(int j=i;j>0;j--){
				if(array[j]>array[j-1]){
					int temp=array[j];
					array[j]=array[j-1];
					array[j-1]=temp;
				}	
			}
		}
		return array;
	}
}
