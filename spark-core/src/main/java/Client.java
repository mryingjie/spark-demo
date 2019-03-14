/**
 * @author ZhengYingjie
 * @time 2019/3/13 11:12
 * @description
 */
public class Client {

    public static void main(String[] args) {

        final People people = new People();
        people.setName("");
        people.setAge(0);


    }

}

class People{

    private String name;

    private Integer age;
    private double salary;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }


    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }
}
