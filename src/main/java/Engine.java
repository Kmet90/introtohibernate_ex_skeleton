import entities.Address;
import entities.Employee;
import entities.Project;
import entities.Town;

import javax.persistence.EntityManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;

public class Engine implements Runnable {

    private final EntityManager entityManager;
    private final BufferedReader bufferedReader;

    public Engine(EntityManager entityManager) {
        this.entityManager = entityManager;
        this.bufferedReader = new BufferedReader(new InputStreamReader(System.in));
    }

    @Override
    public void run() {
        System.out.println("Please, select exercise number:");
        try {
            int exNum = Integer.parseInt(bufferedReader.readLine());

            switch (exNum) {
                case 2 -> ex2ChangeCasing();
                case 3 -> ex3ContainsEmployee();
                case 4 -> ex4EmployeesWithSalaryOver50000();
                case 5 -> ex5EmployeesFromDepartment();
                case 6 -> ex6AddingANewAddressAndUpdatingEmployee();
                case 7 -> ex7AddressesWithEmployeeCount();
                case 8 -> ex8GetEmployeeWithProject();
                case 9 -> ex9FindLatest10Projects();
                case 10 -> ex10IncreaseSalaries();
                case 11 -> ex11FindEmployeesByFirstName();
                case 12 -> ex12EmployeesMaximumSalaries();
                case 13 -> ex13RemoveTowns();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            entityManager.close();
        }
    }

    private void ex13RemoveTowns() throws IOException {
        System.out.println("Please, enter a town:");
        String town = bufferedReader.readLine();

        entityManager.getTransaction().begin();
        Town townToBeDeleted = this.entityManager.createQuery("SELECT t FROM Town t WHERE t.name = :town", Town.class)
                .setParameter("town", town)
                .getSingleResult();
        List <Address> addressesToBeDeleted = this.entityManager
                .createQuery("SELECT a FROM Address a WHERE a.town.name = :town", Address.class)
                .setParameter("town", town)
                .getResultList();

        addressesToBeDeleted.forEach(address -> address.getEmployees().forEach(employee -> employee.setAddress(null)));
        addressesToBeDeleted.forEach(this.entityManager::remove);
        this.entityManager.remove(townToBeDeleted);

        int countDelAddresses = addressesToBeDeleted.size();
        System.out.printf("%d address%s in %s deleted",
                countDelAddresses, countDelAddresses == 1 ? "" : "es", town);

        entityManager.getTransaction().commit();
    }

    private void ex12EmployeesMaximumSalaries() {
        List<Object[]> results = entityManager
                .createQuery("SELECT e.department.name, Max(e.salary) " +
                        "FROM Employee e " +
                        "GROUP BY e.department.name " +
                        "HAVING Max(e.salary) NOT BETWEEN 30000 AND 70000 ", Object[].class)
                .getResultList();
        for (Object[] result : results) {
            System.out.println(result[0] + " " + result[1]);
        }
    }

    private void ex11FindEmployeesByFirstName() throws IOException {
        System.out.println("Please, enter employee first name pattern:");
        String firstNamePattern = bufferedReader.readLine();
        entityManager
                .createQuery("SELECT e FROM Employee e " +
                        "WHERE e.firstName LIKE concat(:pattern, '%')", Employee.class)
                .setParameter("pattern", firstNamePattern)
                .getResultStream()
                .forEach(e -> System.out.printf("%s %s - %s - ($%.2f)%n"
                        , e.getFirstName()
                        , e.getLastName()
                        , e.getJobTitle()
                        , e.getSalary()));
    }

    private void ex10IncreaseSalaries() {
        entityManager.getTransaction().begin();
        int affectedRows = entityManager
                .createQuery("UPDATE Employee e " +
                        "SET e.salary = e.salary * 1.12 " +
                        "WHERE e.department.id IN (1, 2, 4, 11)")
                .executeUpdate();
        entityManager.getTransaction().commit();

        System.out.println("Affected rows: " + affectedRows);

        entityManager
                .createQuery("SELECT e FROM Employee e WHERE e.department.id IN (1, 2, 4, 11)"
                        , Employee.class)
                .getResultStream()
                .forEach(employee -> System.out.printf("%s %s ($%.2f)%n"
                        , employee.getFirstName()
                        , employee.getLastName()
                        , employee.getSalary()));
    }

    private void ex9FindLatest10Projects() {
        List<Project> projects = entityManager
                .createQuery("SELECT p FROM Project p " +
                        "ORDER BY p.name ASC, p.startDate DESC", Project.class)
                .setMaxResults(10)
                .getResultList();

        projects.forEach(project -> System.out.printf("Project name: %s%n" +
                        "\tProject Description: %s%n" +
                        "\tProject Start Date: %s%n" +
                        "\tProject End Date: %s%n",
                project.getName()
                , project.getDescription()
                , project.getStartDate()
                , project.getEndDate()));
    }

    private void ex8GetEmployeeWithProject() throws IOException {
        System.out.println("Please, enter valid employee Id: ");
        int id = Integer.parseInt(bufferedReader.readLine());

        Employee employee = entityManager.find(Employee.class, id);

        System.out.printf("%s %s - %s%n",
                employee.getFirstName(),
                employee.getLastName(),
                employee.getJobTitle());

        employee.getProjects()
                .stream()
                .sorted(Comparator.comparing(Project::getName))
                .forEach(project -> System.out.printf("\t%s%n", project.getName()));
    }

    private void ex7AddressesWithEmployeeCount() {
        List<Address> addresses = entityManager
                .createQuery("SELECT a FROM Address a " +
                        "ORDER BY a.employees.size DESC", Address.class)
                .setMaxResults(10)
                .getResultList();

        addresses.forEach(address -> System.out.printf("%s, %s - %d%n",
                address.getText(), address.getTown().getName(),
                address.getEmployees().size()));

    }

    private void ex6AddingANewAddressAndUpdatingEmployee() throws IOException {
        System.out.println("Please, enter employee last name:");
        String lastName = bufferedReader.readLine();

        Employee employee = entityManager.createQuery("SELECT e " +
                "FROM  Employee  e " +
                "WHERE  e.lastName = :l_name", Employee.class)
                .setParameter("l_name", lastName)
                .getSingleResult();

        Address address = createAddress("Vitoshka 15");

        entityManager.getTransaction().begin();
        employee.setAddress(address);
        entityManager.getTransaction().commit();

    }

    private Address createAddress(String addressText) {
        Address address = new Address();
        address.setText(addressText);

        entityManager.getTransaction().begin();
        entityManager.persist(address);
        entityManager.getTransaction().commit();

        return address;
    }

    private void ex5EmployeesFromDepartment() {
        entityManager.createQuery("SELECT e FROM Employee e " +
                "WHERE e.department.name = :d_name", Employee.class)
                .setParameter("d_name", "Research and Development")
                .getResultStream()
                .forEach(employee -> System.out.printf("%s %s from %s - $%.2f%n"
                        , employee.getFirstName()
                        , employee.getLastName()
                        , employee.getDepartment().getName()
                        , employee.getSalary()));
    }

    private void ex4EmployeesWithSalaryOver50000() {
        entityManager.createQuery("SELECT e FROM Employee e " +
                "WHERE e.salary > :min_salary", Employee.class)
                .setParameter("min_salary", BigDecimal.valueOf(50000L))
                .getResultStream()
                .map(Employee::getFirstName)
                .forEach(System.out::println);
    }

    private void ex3ContainsEmployee() throws IOException {
        System.out.println("Please, enter full employee name:");
        String[] fullName = bufferedReader.readLine().split("\\s+");
        String firstName = fullName[0];
        String lastName = fullName[1];

        Long result = entityManager.createQuery("SELECT count(e) FROM Employee e " +
                "WHERE e.firstName = :f_name AND e.lastName = :l_name", Long.class)
                .setParameter("f_name", firstName)
                .setParameter("l_name", lastName)
                .getSingleResult();
        System.out.println(result == 0 ? "No" : "Yes");
    }

    private void ex2ChangeCasing() {
        entityManager.getTransaction().begin();
        int update = entityManager.createQuery("UPDATE Town t " +
                "SET t.name = upper(t.name) " +
                "WHERE length(t.name) <= 5 ")
                .executeUpdate();
        System.out.println(update);
        entityManager.getTransaction().commit();
    }
}
