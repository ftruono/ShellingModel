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

enum RANGE {
    OUT_OF_RANGE,
    IN_MY_RANGE,
    INVALID
};

enum TYPE_REQUEST {
    REQUEST_INFO,
    RESPONSE_INFO
};

typedef struct {
    int row;
    int col;
    int satisfation;
    int red;
    int blue;
} InitializeMsg;


typedef struct {
    enum STATUS status;
    bool locked;
    int satisfacion;
} City;

typedef struct {
    int pos;
    enum TYPE_REQUEST request;
    City content;
} InfoMsg;

MPI_Datatype make_type_for_initialize_msg();

MPI_Datatype make_type_for_city();

MPI_Datatype make_type_for_info_msg(MPI_Datatype city_type);

void get_input_from_terminal(int *grid_size, int *red_pop, int *blue_pop, int *empty, int *satisfatcion);

void handle_input(char *arg, int *store, bool isGrid);

char decode_enum(enum STATUS status);

//int *reduceProcesses(int processes, int grid_size);

void send_startup_information(InitializeMsg msg, int proc,
                              MPI_Datatype mpi_initialize_message_type);

InitializeMsg create_initialize_message(int row, int col, int satisfaction, int blue, int red);

City **initialize_grid_city(InitializeMsg initialize_struct);

void push_random_values(City **grid_city, int max_row, int col, int content_legth, enum STATUS status);

void print_grid(City **grid_city, int row, int col);

int calculate_offset(int proc, int x, int y, int col, int row);

void check_satisfaction_horizontal(City **grid_city, int i, int j, int pos, int *count_near, int *satisf);

void check_satisfaction_vertical(City **grid_city, int i, int j, int pos, int *count_near, int *satisf);

enum RANGE is_in_my_range(int value, int min_range, int max_range, int max_size);

void check_satisfaction_oblique(City **grid_city, int i, int j, int pos_x, int pos_y, int *count_near, int *satisf);


int main(int argc, char *argv[]) {
    int processes, rank;
    int unsatisfied;
    InitializeMsg startup_msg;
    City **grid;
    MPI_Status status_in_msg;
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &processes);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    srand(time(NULL) + rank);
    MPI_Datatype mpi_initialize_message_type = make_type_for_initialize_msg();
    MPI_Datatype mpi_city_type = make_type_for_city();
    MPI_Datatype mpi_info_msg_type = make_type_for_info_msg(mpi_city_type);

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
        int total_empty = (total_cells * empty) / 100;
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
    MPI_Type_free(&mpi_city_type);
    MPI_Type_free(&mpi_initialize_message_type);
    MPI_Type_free(&mpi_info_msg_type);
    MPI_Finalize();
    return 0;
}


MPI_Datatype make_type_for_initialize_msg() {
    MPI_Datatype types[5] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};
    MPI_Datatype mpi_message_type;
    MPI_Aint offsets[5];
    int blocklengths[5] = {1, 1, 1, 1, 1};
    offsets[0] = offsetof(InitializeMsg, row);
    offsets[1] = offsetof(InitializeMsg, col);
    offsets[2] = offsetof(InitializeMsg, satisfation);
    offsets[3] = offsetof(InitializeMsg, red);
    offsets[4] = offsetof(InitializeMsg, blue);

    MPI_Type_create_struct(5, blocklengths, offsets, types, &mpi_message_type);
    MPI_Type_commit(&mpi_message_type);

    return mpi_message_type;
}


MPI_Datatype make_type_for_city() {
    MPI_Datatype types[3] = {MPI_UNSIGNED_CHAR, MPI_C_BOOL, MPI_INT};
    MPI_Datatype mpi_city;
    MPI_Aint offsets[3];
    int blocklengths[3] = {1, 1, 1};
    offsets[0] = offsetof(City, status);
    offsets[1] = offsetof(City, locked);
    offsets[2] = offsetof(City, satisfacion);

    MPI_Type_create_struct(3, blocklengths, offsets, types, &mpi_city);
    MPI_Type_commit(&mpi_city);

    return mpi_city;
}

MPI_Datatype make_type_for_info_msg(MPI_Datatype city_type) {
    MPI_Datatype types[3] = {MPI_INT, MPI_UNSIGNED_CHAR, city_type};
    MPI_Datatype mpi_info_msg;
    MPI_Aint offsets[3];
    int blocklengths[3] = {1, 1, 1};
    offsets[0] = offsetof(InfoMsg, pos);
    offsets[1] = offsetof(InfoMsg, request);
    offsets[2] = offsetof(InfoMsg, content);

    MPI_Type_create_struct(3, blocklengths, offsets, types, &mpi_info_msg);
    MPI_Type_commit(&mpi_info_msg);

    return mpi_info_msg;
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

InitializeMsg create_initialize_message(int row, int col, int satisfaction, int blue, int red) {
    InitializeMsg msg;
    msg.col = col;
    msg.satisfation = satisfaction;
    msg.blue = blue;
    msg.red = red;
    msg.row = row;
    return msg;
}

void send_startup_information(InitializeMsg msg, int proc,
                              MPI_Datatype mpi_initialize_message_type) {

    //printf("Messaggio di inizializzazione red: %d blue:%d  \n", msg.red, msg.blue);
    MPI_Send(&msg, 1, mpi_initialize_message_type, proc, 0, MPI_COMM_WORLD);
}

//TODO empty non necessario
City **initialize_grid_city(InitializeMsg initialize_struct) {
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


void check_nearest(int proc, City **grid_city, int row, int col, MPI_Datatype mpi_info_msg) {
    int max_range = calculate_offset(proc, row - 1, col - 1, col, row);
    int min_range = calculate_offset(proc, 0, 0, col, row);
    int max_size = col * col;
    for (int i = 0; i < row; ++i) {
        for (int j = 0; j < col; ++j) {
            int pos = calculate_offset(proc, i, j, col,row);
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
            if (is_in_my_range(top, min_range, max_range, max_size) == IN_MY_RANGE) {
                int top_index = i - 1;
                check_satisfaction_vertical(grid_city, i, j, top_index, &count_near, &satisf);
            } else {
                InfoMsg msg;
                msg.request = REQUEST_INFO;

                //come calcolo il processo dalla posizione??
                MPI_Isend()
                //send request to process
            }
            int bottom = pos + col;
            if (is_in_my_range(bottom, min_range, max_range, max_size) == IN_MY_RANGE) {
                int top_index = i + 1;
                check_satisfaction_vertical(grid_city, i, j, top_index, &count_near, &satisf);
            } else {
                //send request to process  => >0 && <col*col
            }

            int nord_ovest = pos - col - 1;
            if (is_in_my_range(nord_ovest, min_range, max_range, max_size) == IN_MY_RANGE) {
                int no_x = i - 1;
                int no_y = j - 1;
                check_satisfaction_oblique(grid_city, i, j, no_x, no_y, &count_near, &satisf);
            } else {

            }
            int nord_east = pos - col + 1;
            if (is_in_my_range(nord_east, min_range, max_range, max_size) == IN_MY_RANGE) {
                int no_x = i - 1;
                int no_y = j + 1;
                check_satisfaction_oblique(grid_city, i, j, no_x, no_y, &count_near, &satisf);
            } else {

            }

            int south_ovest = pos + col - 1;
            if (is_in_my_range(south_ovest, min_range, max_range, max_size) == IN_MY_RANGE) {
                int no_x = i + 1;
                int no_y = j - 1;
                check_satisfaction_oblique(grid_city, i, j, no_x, no_y, &count_near, &satisf);
            } else {

            }
            int south_east = pos + col + 1;
            if (is_in_my_range(south_east, min_range, max_range, max_size) == IN_MY_RANGE) {
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

enum RANGE is_in_my_range(int value, int min_range, int max_range, int max_size) {
    if (value >= min_range && value <= max_range)
        return IN_MY_RANGE;
    else if (value < 0 || value > max_size) {
        return INVALID;
    } else {
        return OUT_OF_RANGE;
    }
}

int calculate_offset(int proc, int x, int y, int col, int row) {
    return (proc * col * row) + (x * col + y);
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