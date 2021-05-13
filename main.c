#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <stdbool.h>
#include <time.h>
#include <math.h>
#include "mpi.h"


enum STATUS {
    RED, BLUE, EMPTY
};

typedef struct {
    int row;
    int col;
    int satisfation;
    int red;
    int blue;
} Initialize_msg;

typedef struct {
    enum STATUS status;
    bool locked;
    int satisfacion;
} City;

MPI_Datatype make_type_for_msg();

void get_input_from_terminal(int *grid_size, int *red_pop, int *blue_pop, int *empty, int *satisfatcion);

void handle_input(char *arg, int *store, bool isGrid);

char decode_enum(enum STATUS status);

//int *reduceProcesses(int processes, int grid_size);

void send_startup_information(Initialize_msg msg, int proc,
                              MPI_Datatype mpi_initialize_message_type);

Initialize_msg create_initialize_message(int row, int col, int satisfaction, int blue, int red);

City **initialize_grid_city(Initialize_msg initialize_struct);

void push_random_values(City **grid_city, int max_row, int col, int content_legth, enum STATUS status);

void print_grid(City **grid_city, int row, int col);

int calculate_offset(int proc, int x, int y, int col);

void check_satisfaction_horizontal(City **grid_city, int i, int j, int pos, int *count_near, int *satisf);

void check_satisfaction_vertical(City **grid_city, int i, int j, int pos, int *count_near, int *satisf);

bool isInMyRange(int value, int min_range, int max_range);

void check_satisfaction_oblique(City **grid_city, int i, int j, int pos_x, int pos_y, int *count_near, int *satisf);


int main(int argc, char *argv[]) {
    int processes, rank;
    int unsatisfied;
    Initialize_msg startup_msg;
    City **grid;
    MPI_Status status_in_msg;
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &processes);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    srand(time(NULL) + rank);
    MPI_Datatype mpi_initialize_message_type = make_type_for_msg();
    printf("%c", BLUE);
    if (rank == 0) {
        int grid_size, red_pop, blue_pop, satisfaction, empty;

        if (argc < 5) {
            get_input_from_terminal(&grid_size, &red_pop, &blue_pop, &empty, &satisfaction);
        } else {
            handle_input(argv[1], &grid_size, true);
            handle_input(argv[2], &blue_pop, false);
            red_pop = 100 - blue_pop;
            handle_input(argv[4], &empty, false);
            handle_input(argv[5], &satisfaction, false);
        }
        int total_cells = grid_size * grid_size;
        int total_empty = (total_cells * empty) / 100.0;
        total_cells -= total_empty;
        int total_blue = (total_cells * blue_pop) / 100;
        int total_red = (total_cells * red_pop) / 100;
        /*printf("celle totali %d \n vuoti %d \n totali-vuoti %d \n blu totali: %d \n rossi totali %d \n",
               grid_size * grid_size, total_empty, total_cells, total_blue, total_red);*/
        //TODO assunzione che processes < grid_size (P<N) - assunto che la perdita di qualche cella non è un problema
        int splitted_blue = total_blue / processes;
        int splitted_red = total_red / processes;
        int splitted_row = grid_size / processes;

        //printf("row %d \n blue x proc %d \n red x proc %d \n", splitted_row, splitted_blue, splitted_red);
        int excluded = grid_size % processes;
        if (excluded != 0) {
            int assigned_processes = rand() % processes;
            int temp_proc_row = splitted_row + excluded;
            if (assigned_processes == 0) {
                startup_msg = create_initialize_message(temp_proc_row, grid_size, satisfaction, splitted_blue,
                                                        splitted_red);
            } else {
                for (int i = 1; i < processes; ++i) {
                    send_startup_information(
                            create_initialize_message(i == assigned_processes ? temp_proc_row : splitted_row, grid_size,
                                                      satisfaction, splitted_blue,
                                                      splitted_red),
                            i,
                            mpi_initialize_message_type
                    );
                }
            }

        } else {
            startup_msg = create_initialize_message(splitted_row, grid_size, satisfaction, splitted_blue,
                                                    splitted_red); // for 1° process

            for (int i = 1; i < processes; ++i) {
                send_startup_information(
                        create_initialize_message(splitted_row, grid_size, satisfaction, splitted_blue, splitted_red),
                        i,
                        mpi_initialize_message_type
                );
            }
        }

    }
    for (int i = 1; i < processes; ++i) {
        MPI_Recv(&startup_msg, 1, mpi_initialize_message_type, 0, 0, MPI_COMM_WORLD, &status_in_msg);
        grid = initialize_grid_city(startup_msg);
        //print_grid(grid, startup_msg.row, startup_msg.col);
        //printf("rank %d - my grid %d %d \n", rank, startup_msg.row, startup_msg.col);
    }
    do {


    } while (unsatisfied > 0);

    free(grid);
    MPI_Type_free(&mpi_initialize_message_type);
    MPI_Finalize();
    return 0;
}


MPI_Datatype make_type_for_msg() {
    MPI_Datatype types[5] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};
    MPI_Datatype mpi_message_type;
    MPI_Aint offsets[5];
    int blocklengths[5] = {1, 1, 1, 1, 1};
    offsets[0] = offsetof(Initialize_msg, row);
    offsets[1] = offsetof(Initialize_msg, col);
    offsets[2] = offsetof(Initialize_msg, satisfation);
    offsets[3] = offsetof(Initialize_msg, red);
    offsets[4] = offsetof(Initialize_msg, blue);

    MPI_Type_create_struct(5, blocklengths, offsets, types, &mpi_message_type);
    MPI_Type_commit(&mpi_message_type);

    return mpi_message_type;
}


void get_input_from_terminal(int *grid_size, int *red_pop, int *blue_pop, int *empty, int *satisfatcion) {
    char grid_string[10], blue_string[3], empty_string[3], satisfaction_string[3];
    printf("Size of grid:\n");
    scanf("%s", grid_string);
    handle_input(grid_string, grid_size, true);

    printf("blue(B) population in %% \n");
    scanf("%s", blue_string);
    handle_input(blue_string, blue_pop, false);
    *red_pop = 100 - *blue_pop;

    printf("Empty space in %% \n");
    scanf("%s", empty_string);
    handle_input(empty_string, empty, false);

    printf("Satisfatcion agent in %% \n");
    scanf("%s", satisfaction_string);
    handle_input(satisfaction_string, satisfatcion, false);
}


void handle_input(char *arg, int *store, bool isGrid) {
    char *p;
    int conv = strtol(arg, &p, 10);
    if (*p != '\0' || (isGrid && conv <= 1) || (!isGrid && (conv < 0 || conv > 100))) {
        printf("There is an error on conversion or invalid range");
        exit(-1);
    } else {
        *store = conv;
    }
}

/*
int *reduceProcesses(int processes, int grid_size) {
    while (processes >= grid_size) {
        processes /= 2;
    }
    int *valid = (int *) malloc(sizeof(int) * processes);
    for (int i = 0; i < processes; ++i) {
        valid[i] = i;
    }
    return valid;
}
*/

Initialize_msg create_initialize_message(int row, int col, int satisfaction, int blue, int red) {
    Initialize_msg msg;
    msg.col = col;
    msg.satisfation = satisfaction;
    msg.blue = blue;
    msg.red = red;
    msg.row = row;
    return msg;
}

void send_startup_information(Initialize_msg msg, int proc,
                              MPI_Datatype mpi_initialize_message_type) {

    //printf("Messaggio di inizializzazione red: %d blue:%d  \n", msg.red, msg.blue);
    MPI_Send(&msg, 1, mpi_initialize_message_type, proc, 0, MPI_COMM_WORLD);
}

//TODO empty non necessario
City **initialize_grid_city(Initialize_msg initialize_struct) {
    //printf("Push random: ");
    City **grid_city = (City **) malloc(initialize_struct.row * sizeof(City));
    for (int i = 0; i < initialize_struct.col; ++i) {
        grid_city[i] = (City *) malloc(initialize_struct.col * sizeof(City));
    }
    for (int i = 0; i < initialize_struct.row; ++i) {
        for (int j = 0; j < initialize_struct.col; ++j) {
            grid_city[i][j].status = EMPTY;
            grid_city[i][j].locked = false;
        }
    }

    push_random_values(grid_city, initialize_struct.row, initialize_struct.col, initialize_struct.red, RED);
    push_random_values(grid_city, initialize_struct.row, initialize_struct.col, initialize_struct.blue, BLUE);

    return grid_city;
}

void push_random_values(City **grid_city, int max_row, int max_col, int content_legth, enum STATUS status) {
    int next = content_legth;
    while (next > 0) {
        int row = rand() % max_row;
        int col = rand() % max_col;
        if (!grid_city[row][col].locked) {
            grid_city[row][col].status = status;
            grid_city[row][col].locked = (status == EMPTY);
            --next;
        }

    }
    //printf("Inizializzato\n");

}


void check_nearest(int proc, City **grid_city, int row, int col) {
    int max_range = calculate_offset(proc, row - 1, col - 1, col);
    int min_range = calculate_offset(proc, 0, 0, col);
    for (int i = 0; i < row; ++i) {
        for (int j = 0; j < col; ++j) {
            int pos = calculate_offset(proc, i, j, col);
            int satisf = 0;
            int count_near = 0;
            int left = j - 1;
            if (left > 0) {
                check_satisfaction_horizontal(grid_city, i, j, left, &count_near, &satisf);
            }
            int rigth = j + 1;
            if (rigth < col) {
                check_satisfaction_horizontal(grid_city, i, j, rigth, &count_near, &satisf);
            }

            int top = pos - col;
            if (isInMyRange(top, min_range, max_range)) {
                int top_index = i - 1;
                check_satisfaction_vertical(grid_city, i, j, top_index, &count_near, &satisf);
            } else {
                //send request to process
            }
            int bottom = pos + col;
            if (isInMyRange(bottom, min_range, max_range)) {
                int top_index = i + 1;
                check_satisfaction_vertical(grid_city, i, j, top_index, &count_near, &satisf);
            } else {
                //send request to process  => >0 && <col*col
            }

            int nord_ovest = pos - col - 1;
            if (isInMyRange(nord_ovest, min_range, max_range)) {
                int no_x = i - 1;
                int no_y = j - 1;
                check_satisfaction_oblique(grid_city, i, j, no_x, no_y, &count_near, &satisf);
            } else {

            }
            int nord_east = pos - col + 1;
            if (isInMyRange(nord_east, min_range, max_range)) {
                int no_x = i - 1;
                int no_y = j + 1;
                check_satisfaction_oblique(grid_city, i, j, no_x, no_y, &count_near, &satisf);
            } else {

            }

            int south_ovest = pos + col - 1;
            if (isInMyRange(south_ovest, min_range, max_range)) {
                int no_x = i + 1;
                int no_y = j - 1;
                check_satisfaction_oblique(grid_city, i, j, no_x, no_y, &count_near, &satisf);
            } else {

            }
            int south_east = pos + col + 1;
            if (isInMyRange(south_east, min_range, max_range)) {
                int no_x = i + 1;
                int no_y = j + 1;
                check_satisfaction_oblique(grid_city, i, j, no_x, no_y, &count_near, &satisf);
            } else {

            }


        }
    }
}

void check_satisfaction_horizontal(City **grid_city, int i, int j, int pos, int *count_near, int *satisf) {
    if (grid_city[i][pos].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == grid_city[i][pos].status) {
            (*satisf)++;
        }
    }
}

void check_satisfaction_vertical(City **grid_city, int i, int j, int pos, int *count_near, int *satisf) {
    if (grid_city[pos][j].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == grid_city[pos][j].status) {
            (*satisf)++;
        }
    }
}

void check_satisfaction_oblique(City **grid_city, int i, int j, int pos_x, int pos_y, int *count_near, int *satisf) {
    if (grid_city[pos_x][pos_y].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == grid_city[pos_x][pos_y].status) {
            (*satisf)++;
        }
    }
}

bool isInMyRange(int value, int min_range, int max_range) {
    return value >= min_range && value <= max_range;
}

int calculate_offset(int proc, int x, int y, int col) {
    return (proc * col) + (x * col + y);
}


void print_grid(City **grid_city, int row, int col) {
    for (int i = 0; i < row; ++i) {
        for (int j = 0; j < col; ++j) {
            printf("%c  ", decode_enum(grid_city[i][j].status));
        }
        printf("\n");
    }
}

char decode_enum(enum STATUS status) {
    switch (status) {
        case BLUE:
            return 'B';
        case RED:
            return 'R';
        case EMPTY:
            return ' ';
    }
}