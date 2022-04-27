<p align="center">
  <img
    src="https://raw.githubusercontent.com/meteostat/meteor/be2caf1cd918cbd959ab881b0ec2af035c1a604b/logo.svg"
    width="300"
  >
</p>

Meteor provides a simple base class for creating automated import, export and maintenance tasks which interact with different Meteostat interfaces. Most tasks import meteorological data from open governmental interfaces into Meteostat's central SQL database. All automated tasks are placed in the `cron` directory.

## Contributing

Meteostat has a strong focus on open source. We rely on coding enthusiasts who are passionate about meteorology to grow our database and collect as much weather and climate data as possible.

### Creating a Task

All tasks extend the `Meteor` base class. Use the following template for creating new tasks:

```py
"""
Module description
"""

from meteor import Meteor, run


class Task(Meteor):
  """
  Task description
  """

  name = 'import.example'

  def main(self) -> None:
    """
    Main script & entry point
    """

    # Your implementation

# Run task
run(Task)
```

You may use additional methods or class attributes to structure your code.

## License

The code of this library is available under the [MIT license](https://opensource.org/licenses/MIT).
