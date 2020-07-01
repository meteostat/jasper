# Meteostat Routines
This repository is a collection of automated routines for importing and managing weather and climate data with Python. Its purpose is the maintenance of the Meteostat database which provides detailed historical time series for thousands of weather stations worldwide.
The data is imported from different governmental interfaces which provide open weather and climate data.
## Contributing
Meteostat is a voluntary initiative that fosters open source. We rely on coding enthusiasts who are passionate about meteorology to grow our database and collect as much weather and climate data as possible.
## Roadmap
To get this project started, we will need to do a few things, beforehand:
* Development of a basis framework for data imports that provides methods for formating and inserting data
* Rewrite of existing [PHP routines](https://github.com/meteostat/routines-legacy) in Python

Once these steps are completed, we can start adding new data sources to the projects. More details about how the development of this repository will be set up are discussed in a [dedicated issue](https://github.com/meteostat/routines/issues/1).
## Legacy Code
Meteostat started as a beginner's coding project, written in PHP, and developed into a large platform for weather statistics. As thousands of users are starting to use the project's web application and API, Meteostat needs to shift its amateur code base to a more professional setup. We decided to go with Python due its popularity and the great ability to work with large amounts of data. However, our legacy PHP code is still available in an [archived repository](https://github.com/meteostat/routines-legacy). You can use the PHP scripts in this repository as an inspiration for your Python implementations.
## Testing
For now we will only do manual testing. However, a proper strategy for code testing will be defined in the near future.
## License
The code of the Meteostat project is available under the [MIT license](https://opensource.org/licenses/MIT).
