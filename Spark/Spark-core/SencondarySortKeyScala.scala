class SecondarySortKeyScala(val first:Int,val sencond:Int) extends Ordered[SecondarySortKeyScala] with Serializable{
  override def compare(that: SecondarySortKeyScala): Int = {
    if(this.first-that.first != 0){
      return this.first-that.first
    }else{
      return this.sencond-that.sencond
    }
  }
}
