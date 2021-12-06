# NoSqlMySql

### Example Insertion

```c#
public class PersonModel
{
  public string FirstName { get; set; }
  public string LastName { get; set; }
  public int Age { get; set; }
  public DateTime DateOfBirth { get; set; }
}

NoSqlClient client = new NoSqlClient("localhost", 3306, "root", "password");
NoSqlDatabase db = client.GetDatabase("AddressBook");
NoSqlCollection collection = db.GetCollection("Contacts");

PersonModel person = new PersonModel {
  FirstName = "Joe",
  LastName = "Bloggs",
  Age = 21,
  DateOfBirth = new DateTime(2000, 12, 6)
};

collection.InsertOne(person);
```

### Example Delete
```c#
NoSqlClient client = new NoSqlClient("localhost", 3306, "root", "password");
NoSqlDatabase db = client.GetDatabase("AddressBook");
NoSqlCollection collection = db.GetCollection("Contacts");

collection.DeleteOne(new Guid("ce76bf13-ba20-49df-89f8-b73094fdad24"));
```

### Example Get
```c#
public class PersonModel
{
  public string FirstName { get; set; }
  public string LastName { get; set; }
  public int Age { get; set; }
  public DateTime DateOfBirth { get; set; }
}

NoSqlClient client = new NoSqlClient("localhost", 3306, "root", "password");
NoSqlDatabase db = client.GetDatabase("AddressBook");
NoSqlCollection collection = db.GetCollection("Contacts");
PersonModel model = collection.GetOne<PersonModel>(new Guid("ce76bf13-ba20-49df-89f8-b73094fdad24"))
```
